package main

import (
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]
	database := Database{}
	databaseFile, err := os.Open(databaseFilePath)
	defer databaseFile.Close()
	database.Init(databaseFile)
	if err != nil {
		log.Fatal(err)
	}

	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Fprintln(os.Stderr, "Logs from your program will appear here!")
	switch command {
	case ".tables":
		tablesString := ""
		for _, page := range database.pages {
			for _, cell := range page.cellContents {
				for _, record := range cell.record {
					tablesString = record.body.tableName + " " + tablesString
				}
			}
		}
		fmt.Println(tablesString)
	case ".dbinfo":

		// Uncomment this to pass the first stage
		fmt.Println("database page size: ", database.pageSize)
		tableCount := uint16(0)
		for _, eachPage := range database.pages {
			tableCount += eachPage.header.numberOfCells
		}
		fmt.Println("number of tables: ", tableCount)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
