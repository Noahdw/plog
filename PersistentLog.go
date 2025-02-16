package persistentlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/cespare/xxhash"
)

type PersistentLog struct {
	file        *os.File
	maxLogIndex int
}

func NewPeristentLog() (*PersistentLog, error) {
	file, err := os.OpenFile("log.dat", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("could not open log %v", err)
	}
	plog := PersistentLog{
		file: file,
	}
	err = plog.readLogFromFile()
	if err != nil {
		return nil, fmt.Errorf("could not read log %v", err)
	}
	return &plog, nil
}

func (p *PersistentLog) StoreValue(value string) {
	dataLen := len(value)

	// Make msg len into byte slice
	dataLenbuf := make([]byte, 4) // for uint32/int32
	binary.BigEndian.PutUint32(dataLenbuf, uint32(dataLen))

	// Write checksum first followed by the (length of value + value)
	checksum := getChecksum([]byte(value), dataLenbuf)
	msgToWrite := append(checksum, dataLenbuf...)
	msgToWrite = append(msgToWrite, []byte(value)...)
	p.file.Write(msgToWrite)
	p.file.Sync()
	p.maxLogIndex += 1
}

func (p *PersistentLog) readLogFromFile() error {
	file, err := os.OpenFile("log.dat", os.O_RDWR, 0666)
	if err != nil {
		return nil
	}
	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("cannot read file size %v", err)
	}
	filesize := fileInfo.Size()

	// Read the full file into rawFileBytes
	rawFileBytes := make([]byte, filesize)
	_, err = file.Read(rawFileBytes)
	if err != nil {
		return fmt.Errorf("cannot read file %v", err)
	}

	const HASHLEN = 8
	const INTLEN = 4
	pos := 0
	lastValidPos := 0
	checksum := make([]byte, HASHLEN)
	rawDataLen := make([]byte, INTLEN)
	// Read all entries from disk
	// checksum - data len - data value
	for pos < int(filesize) {
		// Read the checksum
		if pos+HASHLEN <= int(filesize) {
			copy(checksum, rawFileBytes[pos:pos+HASHLEN])
		} else {
			break
		}
		pos += HASHLEN
		fmt.Printf("checksum: %x\n", checksum)

		// Read the data len
		if pos+INTLEN <= int(filesize) {
			copy(rawDataLen, rawFileBytes[pos:pos+INTLEN])
		} else {
			break
		}
		pos += INTLEN
		fmt.Printf("rawdatalen: %d\n", rawDataLen)

		// Turn binary encoded slice into int
		datalenInt := int(binary.BigEndian.Uint32(rawDataLen[:INTLEN]))
		fmt.Printf("datalen: %d\n", datalenInt)

		dataContent := make([]byte, datalenInt)
		// Read the data
		if pos+datalenInt <= int(filesize) {
			copy(dataContent, rawFileBytes[pos:pos+datalenInt])
		} else {
			fmt.Printf("pos+datalen err: %d %d\n", pos+datalenInt, filesize)
			break
		}
		fmt.Printf("content: %s\n", dataContent)

		computedChecksum := getChecksum(dataContent, rawDataLen)
		if !bytes.Equal(computedChecksum, checksum) {
			break
		}

		p.maxLogIndex += 1
		pos += datalenInt
		lastValidPos = pos
	}

	file.Close()
	// Clean up (remove) entries that are invalid or come after an invalid one
	if lastValidPos < int(filesize) {
		fmt.Printf("Truncate: %d %d\n", lastValidPos, filesize)
		err = os.Truncate("log.dat", int64(lastValidPos))
		if err != nil {
			fmt.Printf("trunc fail %s\n", err.Error())
			return nil
		}
	}
	return nil
}

func (p *PersistentLog) GetMaxLogIndex() int {
	return p.maxLogIndex
}

func (p *PersistentLog) Close() {
	if p.file != nil {
		p.file.Close()
	}
}

func getChecksum(value []byte, valueLen []byte) []byte {
	// Crease a checksum based on the concat of :
	// 1. Length of the value, expressed in a byte buffer of size 4 (int)
	// 2. The value to be stored
	hashFunc := xxhash.New()
	msgToHash := append(valueLen, []byte(value)...)
	var hash []byte
	hashFunc.Write(msgToHash)
	hash = hashFunc.Sum(hash)
	hashFunc.Reset()
	return hash
}
