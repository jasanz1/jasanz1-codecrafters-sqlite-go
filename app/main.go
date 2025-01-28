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

type Page struct {
	header       PageHeader
	cellPointers []uint16
	cellContents []byte
}
type PageHeader struct {
	pageType           uint8
	firstFreeblock     uint16
	numberOfCells      uint16
	startOfCellContent uint16
	freeBytes          uint8
	firstMostPointer   *uint32
}
type DatabaseFile struct {
	header   []byte
	pageSize uint16
	pages    []Page
}

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]
	database := DatabaseFile{}
	databaseFile, err := os.Open(databaseFilePath)
	database.initHeaderPage(databaseFile)
	if err != nil {
		log.Fatal(err)
	}

	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Fprintln(os.Stderr, "Logs from your program will appear here!")
	switch command {
	case ".dbinfo":

		// Uncomment this to pass the first stage
		fmt.Println("database page size: ", database.pageSize)
		fmt.Println("number of tables: ", database.pages[0].header.numberOfCells)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

func (database *DatabaseFile) initHeaderPage(databaseFile *os.File) {

	database.header = make([]byte, 100)
	_, err := databaseFile.Read(database.header)
	if err != nil {
		log.Fatal(err)
	}

	if err := binary.Read(bytes.NewReader(database.header[16:18]), binary.BigEndian, &database.pageSize); err != nil {
		fmt.Println("Failed to read integer:", err)
		return
	}
	_, err = databaseFile.Seek(0, 0)

	page := make([]byte, database.pageSize)
	_, err = databaseFile.Read(page)
	var pageHeaderBytes [12]byte
	if err := binary.Read(bytes.NewReader(page[100:112]), binary.BigEndian, &pageHeaderBytes); err != nil {
		fmt.Println("Failed to read integer:", err)
		return
	}
	firstMostPointer := new(uint32)
	*firstMostPointer = binary.BigEndian.Uint32(pageHeaderBytes[8:12])
	pageHeader := PageHeader{
		pageType:           pageHeaderBytes[0],
		firstFreeblock:     binary.BigEndian.Uint16(pageHeaderBytes[1:3]),
		numberOfCells:      binary.BigEndian.Uint16(pageHeaderBytes[3:5]),
		startOfCellContent: binary.BigEndian.Uint16(pageHeaderBytes[5:7]),
		freeBytes:          pageHeaderBytes[7],
		firstMostPointer:   firstMostPointer,
	}

	database.pages = append(database.pages, Page{pageHeader, nil, nil})

}
