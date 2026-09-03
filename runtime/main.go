package main

import (
	"context"
	"fmt"
	"log"

	"pgstream/replication"
)

func main() {
	connString := "postgres://postgres:learnpass@localhost:5432/streamdb?sslmode=disable"

	conn, err := replication.Connect(connString, "todo_slot", "todo_publication")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(context.Background())

	fmt.Println("✅ connected in replication mode")

	startLSN, err := replication.IdentifySystem(conn)
	if err != nil {
		log.Fatal(err)
	}

	err = replication.Stream(conn, "todo_slot", "todo_publication", startLSN)
	if err != nil {
		log.Fatal(err)
	}
}