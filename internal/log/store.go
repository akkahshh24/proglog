package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// encoding in which bytes will be stored in file
var encoding = binary.BigEndian

// number of bytes used to store the record's length
// uint64 requires 8 bytes
const lenWidth = 8

// store - a file to store records in
// wrapper around a file to write and read bytes to and from the file.
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	//get the file information like size
	fileInfo, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fileInfo.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append persists the given bytes to the store
// Returns the number of bytes written, position of record and an error if any
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	// write the length of the record in the buffer to know how many bytes to read
	// use buffered writer to reduce system calls
	if err := binary.Write(s.buf, encoding, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// w is the number of bytes written to the file
	// write the record in the buffer
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// number of bytes of the record + 8 bytes to store the length of the record
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

// Read returns the record stored at the given position
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// flush the buffer data to underlying writer
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// read 8 bytes from the given position to get the length of the record
	size := make([]byte, lenWidth)
	// ReadAt is useful for concurrent reads as it does not alter current file offset
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// read the record
	record := make([]byte, encoding.Uint64(size))
	if _, err := s.File.ReadAt(record, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return record, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

// Close persists the buffered data before closing the file
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}

	return s.File.Close()
}
