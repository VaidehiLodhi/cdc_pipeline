package replication

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type WalEvent struct {
	Table   string
	Action  string
	Columns map[string]string
}

func Connect(connString, slotName, pubName string) (*pgconn.PgConn, error) {
	conn, err := pgconn.Connect(context.Background(), connString+"&replication=database")
	if err != nil {
		return nil, fmt.Errorf("connect failed : %w", err)
	}

	return conn, nil
}

func IdentifySystem(conn *pgconn.PgConn) (pglogrepl.LSN, error) {
	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		return 0, fmt.Errorf("identify system failed: %w", err)
	}

	fmt.Printf("System identifies : xlogpos=%s, dbname=%s\n", sysident.XLogPos, sysident.DBName)
	return sysident.XLogPos, nil
}

func Stream(conn *pgconn.PgConn, slotName, pubName string, startLSN pglogrepl.LSN) error {
	ctx := context.Background()

	err := pglogrepl.StartReplication(ctx, conn, slotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", pubName)},
	})
	if err != nil {
		return fmt.Errorf("start replication failed: %w", err)
	}

	fmt.Println("streaming liveee - go make a change")

	currentLSN := startLSN
	standbyMessageTimeout := 10 * time.Second
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	// NEW: remembers each table's column shape, keyed by relation ID
	relations := map[uint32]*pglogrepl.RelationMessage{}

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: currentLSN,
			})
			if err != nil {
				return fmt.Errorf("send standby status failed: %w", err)
			}
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		receiveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		rawMsg, err := conn.ReceiveMessage(receiveCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("receive message failed: %w", err)
		}

		//type assertion : is this raw msg specifically a CopyData msg
		// the wrapper postgres uses for all replication stream data
		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		if len(msg.Data) == 0 {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			continue

		case pglogrepl.XLogDataByteID:
			//ParseXLogData unwraps this into an XLogData sturct
			// which has : 
			// xld.WALStart : the LSN this data begins at
			// xld.WALData : the actual raw bytes of the WAL entry itself, the byte soup 
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				log.Printf("parse error: %v", err)
				continue
			}
			currentLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				log.Printf("logical parse error: %v", err)
				continue
			}

			switch m := logicalMsg.(type) {
			// postgres sends a RelationMessage :here's the shape of the orders table: column 0 is order_id, column 1 is item," etc. — before it ever sends you an actual insert/update/delete for that table. 
			// this is stored in this relations[m.RelationID] = m
			// so later when an InsertMessage arrives, which contains raw, unlabled values in order(Widget, 5, pending, timestamp — no field names attached) — buildEvent can look up which name goes with which position:
			case *pglogrepl.RelationMessage:
				relations[m.RelationID] = m

			case *pglogrepl.InsertMessage:
				event := buildEvent(relations, "insert", m.RelationID, m.Tuple)
				fmt.Printf("\n✨ INSERT on %s: %+v\n", event.Table, event.Columns)

			case *pglogrepl.UpdateMessage:
				event := buildEvent(relations, "update", m.RelationID, m.NewTuple)
				fmt.Printf("\n🔄 UPDATE on %s: %+v\n", event.Table, event.Columns)

			case *pglogrepl.DeleteMessage:
				event := buildEvent(relations, "delete", m.RelationID, m.OldTuple)
				fmt.Printf("\n🗑️  DELETE on %s: %+v\n", event.Table, event.Columns)
			}
		}
	}
}

func buildEvent(relations map[uint32]*pglogrepl.RelationMessage, action string, relationID uint32, tuple *pglogrepl.TupleData) WalEvent {
	rel := relations[relationID]
	columns := map[string]string{}

	if rel != nil && tuple != nil {
		for i, col := range tuple.Columns {
			if i >= len(rel.Columns) {
				continue
			}
			colName := rel.Columns[i].Name
			if col.DataType == 'n' {
				columns[colName] = "NULL"
			} else {
				columns[colName] = string(col.Data)
			}
		}
	}

	table := "unknown"
	if rel != nil {
		table = rel.RelationName
	}

	return WalEvent{
		Table:   table,
		Action:  action,
		Columns: columns,
	}
}