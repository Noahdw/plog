package persistentlog

import (
	"os"
	"testing"
)

var tests = []struct {
	value string
}{
	{value: "this is a test"},
	{value: "this is a "},
	{value: "this is"},
	{value: "123456789123"},
	{value: "123456789"},
	{value: "         "},
}

func TestStoreValue(t *testing.T) {
	temp, err := os.CreateTemp("", "plog")
	if err != nil {
		t.Errorf("Creating temp file")
		return
	}
	defer os.Remove(temp.Name())

	plog, err := NewPeristentLog(temp.Name())
	if err != nil {
		t.Errorf("Creating persistent log")
		return
	}

	// Test storing a value
	for index, test := range tests {
		expectedIndex := index + 1
		plog.StoreValue(test.value)
		actualIndex := plog.MaxLogIndex()
		if actualIndex != expectedIndex {
			t.Errorf("Wrong log index when storing: expected %d, actual %d", expectedIndex, actualIndex)
		}
	}

	// Test reading from an existing
	plog2, err := NewPeristentLog(temp.Name())
	if err != nil {
		t.Errorf("Creating persistent log")
		return
	}
	actualIndex := plog2.MaxLogIndex()
	expectedIndex := len(tests)
	if actualIndex != expectedIndex {
		t.Errorf("Wrong log index when reading: expected %d, actual %d", expectedIndex, actualIndex)
	}
}

func TestCorruption(t *testing.T) {
	temp, err := os.CreateTemp("", "plog")
	if err != nil {
		t.Errorf("Creating temp file")
		return
	}
	defer os.Remove(temp.Name())

	plog, err := NewPeristentLog(temp.Name())
	if err != nil {
		t.Errorf("Creating persistent log")
		return
	}

	// Corrupt an entry
	// The entry before it should be OK
	corruptIndex := 3
	for index, test := range tests {
		if index+1 == corruptIndex {
			err := plog.storeCorruptedValue(test.value, 3)
			if err != nil {
				t.Errorf("Storing corrupted value")
			}
		} else {
			plog.StoreValue(test.value)
		}
	}

	plog2, err := NewPeristentLog(temp.Name())
	if err != nil {
		t.Errorf("Creating persistent log")
		return
	}
	actualIndex := plog2.MaxLogIndex()
	expectedIndex := corruptIndex - 1
	if actualIndex != expectedIndex {
		t.Errorf("Wrong log index when reading corrupt log: expected %d, actual %d", expectedIndex, actualIndex)
	}
}
