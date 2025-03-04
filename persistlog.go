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
	filename    string
	maxLogIndex int
}

func NewPeristentLog(filename string) (*PersistentLog, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("could not open log %v", err)
	}
	plog := PersistentLog{
		file:     file,
		filename: filename,
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
	checksum := getChecksum(dataLenbuf, []byte(value))
	msgToWrite := append(checksum, dataLenbuf...)
	msgToWrite = append(msgToWrite, []byte(value)...)
	p.file.Write(msgToWrite)
	p.file.Sync()
	p.maxLogIndex += 1
}

func (p *PersistentLog) MaxLogIndex() int {
	return p.maxLogIndex
}

func (p *PersistentLog) Close() {
	if p.file != nil {
		p.file.Close()
	}
}

func (p *PersistentLog) readLogFromFile() error {
	file, err := os.OpenFile(p.filename, os.O_RDWR, 0666)
	if err != nil {
		return nil
	}
	defer file.Close()
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

		computedChecksum := getChecksum(rawDataLen, dataContent)
		if !bytes.Equal(computedChecksum, checksum) {
			break
		}

		p.maxLogIndex += 1
		pos += datalenInt
		lastValidPos = pos
	}

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

// For use by tests
// bytesToRemove is the length that will get removed from disk, corrupting the entry
func (p *PersistentLog) storeCorruptedValue(value string, bytesToRemove int64) error {
	p.StoreValue(value)

	fileInfo, err := os.Stat(p.file.Name())
	if err != nil {
		return fmt.Errorf("error getting file info: %w", err)
	}
	currentSize := fileInfo.Size()

	// Calculate new size
	newSize := currentSize - bytesToRemove
	if newSize < 0 {
		return fmt.Errorf("cannot remove %d bytes, file is only %d bytes", bytesToRemove, currentSize)
	}

	// Truncate the file to the new size
	err = os.Truncate(p.file.Name(), newSize)
	if err != nil {
		return fmt.Errorf("error truncating file: %w", err)
	}
	p.file.Sync()
	return nil
}

func getChecksum(valueLen []byte, value []byte) []byte {
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
