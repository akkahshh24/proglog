package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/akkahshh24/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64 // to know new record offset and calculate relative offsets
	config                 Config // to check if the segment is full
}

// Called when the current active segment hits its max size
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	// create a file for store
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	// create a store
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// create a file for index
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	// create an index
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	// check the offset of the previous record
	// if the file is empty, set the next offset as the base offset
	// TODO: understand logic of -1 in debug mode
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

// Append writes the given record to the segment and returns it's offset
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur

	p, err := proto.Marshal(record)
	if err != nil {
		return 0, nil
	}

	// append the record to the store
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, nil
	}

	// add an entry in the index
	// index offsets are relative to the base offset
	if err = s.index.Write(
		uint32(s.nextOffset-s.baseOffset),
		pos,
	); err != nil {
		return 0, nil
	}

	// increment the next offset for the next append call
	s.nextOffset++
	return cur, nil
}

// Read returns the record for the given offset
func (s *segment) Read(off uint64) (*api.Record, error) {
	// calculate relative offset and read index entry
	// get the position of the record
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	// get the record from the store
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	if err != nil {
		return nil, err
	}

	return record, nil
}

// IsMaxed returns whether the segment has reached it's max size
// Small number of long logs: store will be full
// Large number of small logs: index will be full
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Close closes the index and store files
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}

	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}

// Remove closes the segment and removes the index and store files
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}

	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}

	return nil
}
