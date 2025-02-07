package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
)

type bodyType uint8

const (
	null bodyType = iota
	integer
	float
	int0
	int1
	reserved1
	reserver2
	blob
	bodyString
)

type bodyFormat struct {
	size     uint64
	bodyType bodyType
}

func makebodyFormat(i uint64) bodyFormat {
	switch i {
	case 0:
		return bodyFormat{0, null}
	case 1:
		return bodyFormat{1, integer}
	case 2:
		return bodyFormat{2, integer}
	case 3:
		return bodyFormat{3, integer}
	case 4:
		return bodyFormat{4, integer}
	case 5:
		return bodyFormat{6, integer}
	case 6:
		return bodyFormat{8, integer}
	case 7:
		return bodyFormat{8, float}
	case 8:
		return bodyFormat{1, int0}
	case 9:
		return bodyFormat{1, int1}
	case 10:
		return bodyFormat{0, reserved1}
	case 11:
		return bodyFormat{0, reserver2}
	default:
		if i >= 12 {
			switch i % 2 {
			case 0:
				return bodyFormat{(i - uint64(12)) / 2, blob}
			case 1:

				return bodyFormat{(i - uint64(13)) / 2, bodyString}
			}
		}
		panic("Unknown body format")
	}
}

type Body struct {
	name      string
	tableType string
	tableName string
	rootPage  uint64
	schema    string
}

type Record struct {
	headerSize       uint64
	headerSerialCode bodyFormat
	body             Body
}
type Cell struct {
	size   uint64
	rowid  uint64
	record []Record
}
type Page struct {
	header       PageHeader
	cellPointers []uint64
	cellContents []Cell
}
type PageHeader struct {
	pageNumber         uint32
	pageType           uint8
	firstFreeblock     uint16
	numberOfCells      uint16
	startOfCellContent uint16
	freeBytes          uint8
	firstMostPointer   *uint32
}
type Database struct {
	header   []byte
	pageSize uint16
	pages    []Page
}

func (database *Database) Init(databaseFile *os.File) {
	database.initHeaderPage(databaseFile)
	// pageBytes := make([]byte, database.pageSize)
	// _, _ = databaseFile.Read(pageBytes)
	// database.makePage(pageBytes)
}

func makePageHeader(pageBytes []byte, pageNumber uint32) (PageHeader, error) {

	var pageHeaderBytes [12]byte
	if err := binary.Read(bytes.NewReader(pageBytes[:12]), binary.BigEndian, &pageHeaderBytes); err != nil {
		fmt.Println("Failed to read integer:", err)
		return PageHeader{}, err
	}
	pageType := pageHeaderBytes[0]
	firstMostPointer := new(uint32)
	if pageType == 0x05 || pageType == 0x02 {
		*firstMostPointer = binary.BigEndian.Uint32(pageHeaderBytes[8:12])
	} else {
		firstMostPointer = nil
	}
	pageHeader := PageHeader{
		pageNumber:         pageNumber,
		pageType:           pageType,
		firstFreeblock:     binary.BigEndian.Uint16(pageHeaderBytes[1:3]),
		numberOfCells:      binary.BigEndian.Uint16(pageHeaderBytes[3:5]),
		startOfCellContent: binary.BigEndian.Uint16(pageHeaderBytes[5:7]),
		freeBytes:          pageHeaderBytes[7],
		firstMostPointer:   firstMostPointer,
	}
	return pageHeader, nil
}

func (database *Database) makePage(pageBytes []byte) (Page, error) {

	var pageStartOffset uint16 = 0
	if len(database.pages) == 0 {
		pageStartOffset = 100
	}

	pageHeader, err := makePageHeader(pageBytes[pageStartOffset:], uint32(len(database.pages)))
	var page Page
	page.header = pageHeader
	var bTreeHeaderSize uint16
	if pageHeader.firstMostPointer != nil {
		bTreeHeaderSize = 12
	} else {
		bTreeHeaderSize = 8
	}
	cellPointers := make([]byte, pageHeader.numberOfCells*2)
	if err := binary.Read(bytes.NewReader(pageBytes[pageStartOffset+bTreeHeaderSize:pageStartOffset+bTreeHeaderSize+pageHeader.numberOfCells*2]), binary.BigEndian, &cellPointers); err != nil {
		fmt.Println("Failed to read integer:", err)
		panic(err)
	}
	page.cellPointers = make([]uint64, pageHeader.numberOfCells)
	for i := 0; i < len(page.cellPointers); i++ {
		page.cellPointers[i] = uint64(binary.BigEndian.Uint16(cellPointers[(i * 2) : (i*2)+2]))
	}
	slices.Reverse(page.cellPointers)
	page, err = readPage(pageBytes, page)
	if err != nil {
		fmt.Println("Failed to read page:", err)
		return Page{}, err
	}
	return page, nil
}

func (database *Database) initHeaderPage(databaseFile *os.File) {

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

	pageBytes := make([]byte, database.pageSize)
	_, err = databaseFile.Read(pageBytes)
	page, err := database.makePage(pageBytes)
	database.pages = append(database.pages, page)
}

func readPage(pageBytes []byte, page Page) (Page, error) {
	//variant decoding
	for _, cellPointer := range page.cellPointers {
		totalOffset := uint64(0)
		recordSize, recordSizeOffset := readVariant(pageBytes, cellPointer, totalOffset)
		totalOffset += recordSizeOffset
		rowid, rowidOffset := readVariant(pageBytes, cellPointer, totalOffset)
		totalOffset += rowidOffset
		headerSize, headerSizeOffset := readVariant(pageBytes, cellPointer, totalOffset)
		totalOffset += headerSizeOffset
		headerSize-- // subtract 1 because the first byte is the is the size byte and that not included in the array
		headerSerialcodes := make([]bodyFormat, 0, headerSize)
		for i := uint64(0); i < headerSize; {
			headerSerialInt, headerSerialCodeOffset := readVariant(pageBytes, cellPointer, totalOffset)
			headerSerialCode := makebodyFormat(headerSerialInt)
			headerSerialcodes = append(headerSerialcodes, headerSerialCode)
			totalOffset += headerSerialCodeOffset
			i += headerSerialCodeOffset
		}
		recordParts := make([]interface{}, len(headerSerialcodes))
		for i, v := range headerSerialcodes {
			record, size := readRecord(pageBytes, cellPointer, totalOffset, v)
			recordParts[i] = record
			totalOffset += size
		}
		var name, tableType, tableName, schema string
		var rootPage uint64
		body := Body{}
		if len(recordParts) == 5 {
			if value, ok := recordParts[0].(string); ok {
				tableType = value
			}
			if value, ok := recordParts[1].(string); ok {
				name = value
			}
			if value, ok := recordParts[2].(string); ok {
				tableName = value
			}
			if value, ok := recordParts[3].(uint64); ok {
				rootPage = value
			}
			if value, ok := recordParts[4].(string); ok {
				schema = value
			}
			body = Body{name, tableType, tableName, rootPage, schema}
		}
		record := Record{headerSize, makebodyFormat(headerSize), body}
		records := []Record{record}
		cell := Cell{recordSize, rowid, records}
		page.cellContents = append(page.cellContents, cell)
	}
	return page, nil

}

func readRecord(pageBytes []byte, start uint64, offset uint64, headerSerialCode bodyFormat) (interface{}, uint64) {
	var output interface{}
	size := headerSerialCode.size
	if start+offset+size > uint64(len(pageBytes)) {
		panic(fmt.Sprintf("Attempting to read beyond bounds: start=%d, offset=%d, size=%d, pageBytes length=%d", start, offset, size, len(pageBytes)))
	}
	switch headerSerialCode.bodyType {
	case null:
		return nil, 1
	case bodyString:
		resultByte := make([]byte, size)
		err := binary.Read(bytes.NewReader(pageBytes[start+offset:start+offset+size]), binary.BigEndian, &resultByte)
		result := string(resultByte)
		if err != nil {
			panic(err)
		}
		output = result
	case blob:
		result := make([]byte, size)
		err := binary.Read(bytes.NewReader(pageBytes[start+offset:start+offset+size]), binary.BigEndian, &result)
		if err != nil {
			panic(err)
		}
		output = result
	case float:
		var result float64
		err := binary.Read(bytes.NewReader(pageBytes[start+offset:start+offset+size]), binary.BigEndian, &result)
		if err != nil {
			panic(err)
		}
		output = result
	case int0:
		output = 0
	case int1:
		output = 1
	default:

		var err error
		switch size {
		case 1:
			result := uint8(0)
			err = binary.Read(bytes.NewReader(pageBytes[start+offset:start+offset+size]), binary.BigEndian, &result)
			output = result
		case 2:
			result := uint16(0)
			err = binary.Read(bytes.NewReader(pageBytes[start+offset:start+offset+size]), binary.BigEndian, &result)
			output = result
		case 4:
			result := uint32(0)
			err = binary.Read(bytes.NewReader(pageBytes[start+offset:start+offset+size]), binary.BigEndian, &result)
			output = result
		case 6:
			result := uint64(0)
			err = binary.Read(bytes.NewReader(pageBytes[start+offset:start+offset+size]), binary.BigEndian, &result)
			output = result
		case 8:
			result := uint64(0)
			err = binary.Read(bytes.NewReader(pageBytes[start+offset:start+offset+size]), binary.BigEndian, &result)
			output = result
		default:
			panic(fmt.Sprintf("Invalid integer size: %d", size))
		}
		if value, ok := output.(uint64); size == 6 && ok {
			output = uint64(value) >> 2
		}
		if err != nil {
			panic(err)
		}
	}
	return output, size
}

func readVariant(bytes []byte, start uint64, offset uint64) (uint64, uint64) {
	result, resultOffset, err := DecodeVarint(bytes[start+offset : start+offset+9])
	if err != nil {
		panic(err)
	}
	return result, uint64(resultOffset + 1)

}

func EncodeVarint(value uint64) []byte {
	var buf bytes.Buffer
	for {
		// Take the lower 7 bits of the value
		b := byte(value & 0x7F)
		value >>= 7

		// If there are more bits to encode, set the high-order bit
		if value != 0 {
			b |= 0x80
		}

		// Write the byte to the buffer
		buf.WriteByte(b)

		// If no more bits to encode, break
		if value == 0 {
			break
		}
	}
	return buf.Bytes()
}

// DecodeVarint decodes a varint-encoded byte slice into a 64-bit integer.
func DecodeVarint(data []byte) (uint64, uint64, error) {
	var result uint64
	var shift uint
	for i, b := range data {
		// Extract the lower 7 bits and shift them into the result
		result |= (result << shift) + uint64(b&0x7F)
		shift += 7

		// If the high-order bit is not set, we're done
		if b&0x80 == 0 {
			return result, uint64(i), nil
		}

		// If we've processed more than 9 bytes, it's invalid
		if i >= 9 {
			return 0, 0, fmt.Errorf("varint is too long")
		}
	}
	return 0, 0, io.ErrUnexpectedEOF
}
