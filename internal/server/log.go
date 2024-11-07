package server

import (
	"fmt"
	"sync"
)

type Log struct {
	mu sync.Mutex
	records []Record
}

type Record struct {
	Value []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

func NewLog() *Log {
	return &Log{}
}

var ErrOffsetNotFound = fmt.Errorf("offset not found")

func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// set the offset
	record.Offset = uint64(len(c.records))

	// append the record
	c.records = append(c.records, record)

	// return offset
	return record.Offset, nil
}

func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// check if offset exists
	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}

	// return record
	return c.records[offset], nil
}
