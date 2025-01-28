package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

type DatabaseFile struct {
	header   []byte
	pageSize uint16
}

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]
	database := DatabaseFile{}
	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	database.header = make([]byte, 100)

	_, err = databaseFile.Read(database.header)
	if err != nil {
		log.Fatal(err)
	}

	if err := binary.Read(bytes.NewReader(database.header[16:18]), binary.BigEndian, &database.pageSize); err != nil {
		fmt.Println("Failed to read integer:", err)
		return
	}
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Fprintln(os.Stderr, "Logs from your program will appear here!")
	switch command {
	case ".dbinfo":

		// Uncomment this to pass the first stage
		fmt.Printf("database page size: %v", database.pageSize)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
