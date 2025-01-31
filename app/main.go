package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

type bodyFormat interface {
	size() uint64
}

// prolly a better way to do this
type null struct{ typeSize uint64 }

func (x null) size() uint64 { return x.typeSize }

type size8 struct{ typeSize uint64 }

func (x size8) size() uint64 { return x.typeSize }

type size16 struct{ typeSize uint64 }

func (x size16) size() uint64 { return x.typeSize }

type size24 struct{ typeSize uint64 }

func (x size24) size() uint64 { return x.typeSize }

type size32 struct{ typeSize uint64 }

func (x size32) size() uint64 { return x.typeSize }

type size48 struct{ typeSize uint64 }

func (x size48) size() uint64 { return x.typeSize }

type size64 struct{ typeSize uint64 }

func (x size64) size() uint64 { return x.typeSize }

type float struct{ typeSize uint64 }

func (x float) size() uint64 { return x.typeSize }

type int0 struct{ typeSize uint64 }

func (x int0) size() uint64 { return x.typeSize }

type int1 struct{ typeSize uint64 }

func (x int1) size() uint64 { return x.typeSize }

type reserved1 struct{}

func (x reserved1) size() uint64 { panic("Reserved1 not implemented") }

type reserver2 struct{}

func (x reserver2) size() uint64 { panic("Reserver2 not implemented") }

type blob struct{ typeSize uint64 }

func (x blob) size() uint64 { return x.typeSize }

type bodyString struct{ typeSize uint64 }

func (x bodyString) size() uint64 { return x.typeSize }

func makebodyFormat(i uint64) bodyFormat {
	switch i {
	case 0:
		return null{0}
	case 1:
		return size8{1}
	case 2:
		return size16{2}
	case 3:
		return size24{3}
	case 4:
		return size32{4}
	case 5:
		return size48{6}
	case 6:
		return size64{8}
	case 7:
		return float{8}
	case 8:
		return int0{1}
	case 9:
		return int1{1}
	case 10:
		return reserved1{}
	case 11:
		return reserver2{}
	default:
		if i >= 12 {
			switch i % 2 {
			case 0:
				return blob{(i - uint64(12)) / 2}
			case 1:

				return blob{(i - uint64(13)) / 2}
			}
		}
		panic("Unknown body format")
	}
}

type Record struct {
	headerSize       uint64
	headerSerialCode bodyFormat
	body             interface{}
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

	pageBytes := make([]byte, database.pageSize)
	_, err = databaseFile.Read(pageBytes)
	var pageHeaderBytes [12]byte
	if err := binary.Read(bytes.NewReader(pageBytes[100:112]), binary.BigEndian, &pageHeaderBytes); err != nil {
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
	var page Page
	cellPointers := make([]byte, pageHeader.numberOfCells*2)
	if err := binary.Read(bytes.NewReader(pageBytes[112:112+pageHeader.numberOfCells*2]), binary.BigEndian, &cellPointers); err != nil {
		fmt.Println("Failed to read integer:", err)
		return
	}

	page.cellPointers = make([]uint64, pageHeader.numberOfCells)
	for i := 0; i < len(page.cellPointers); i++ {
		page.cellPointers[i] = uint64(binary.BigEndian.Uint16(cellPointers[(i * 2) : (i*2)+2]))
	}
	//variant decoding
	totalOffset := uint64(0)
	recordSize, recordSizeOffset := readVariant(pageBytes, page.cellPointers[0], totalOffset)
	totalOffset += recordSizeOffset
	rowid, rowidOffset := readVariant(pageBytes, page.cellPointers[0], totalOffset)
	totalOffset += rowidOffset
	headerSize, headerSizeOffset := readVariant(pageBytes, page.cellPointers[0], totalOffset)
	totalOffset += headerSizeOffset
	headerSerialcodes := make([]bodyFormat, 0, headerSize-1)
	for i := uint64(0); i < headerSize-1; {
		headerSerialInt, headerSerialCodeOffset := readVariant(pageBytes, page.cellPointers[0], totalOffset)
		headerSerialCode := makebodyFormat(headerSerialInt)
		if err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}
		headerSerialcodes = append(headerSerialcodes, headerSerialCode)
		totalOffset += headerSerialCodeOffset
		i += headerSerialCodeOffset
	}
	recordBody := make([]interface{}, len(headerSerialcodes))
	fmt.Println(headerSerialcodes)
	for i, v := range headerSerialcodes {
		record, size := readRecord(pageBytes, page.cellPointers[0], totalOffset, v, headerSize)
		recordBody[i] = record
		totalOffset += size
	}
	_ = rowid
	recordSize = recordSize + headerSize
	fmt.Println(recordBody)
	database.pages = append(database.pages, page)

}

func readRecord(pageBytes []byte, start uint64, offset uint64, headerSerialCode bodyFormat, headerSize uint64) (Record, uint64) {
	fmt.Println(start, offset, headerSerialCode, headerSize)
	var output interface{}
	size := headerSerialCode.size()
	switch headerSerialCode.(type) {
	case null:
		return Record{headerSize, makebodyFormat(0), nil}, 1
	case bodyString:
		var result []rune
		err := binary.Read(bytes.NewReader(pageBytes[start+offset:start+offset+size]), binary.BigEndian, &result)
		fmt.Println(result)
		if err != nil {
			panic(err)
		}
		output = result
	case blob:
		var result []byte
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
		var result uint64
		err := binary.Read(bytes.NewReader(pageBytes[start+offset:start+offset+size]), binary.BigEndian, &result)
		if err != nil {
			panic(err)
		}
		output = result
	}
	fmt.Println(output)
	return Record{headerSize, makebodyFormat(size), output}, size
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
