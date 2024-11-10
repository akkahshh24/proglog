package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	record = []byte("nihonjin")
	width  = uint64(len(record) + lenWidth)
)

func TestStoreAppendRead(t *testing.T) {
	// create a store with a temp file
	f, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	// append and read
	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	// create the store again and read from it
	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func TestStoreClose(t *testing.T) {
	// create a store with a temp file
	f, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	// append to file
	n, _, err := s.Append(record)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	// close will flush the buffered data to file
	err = s.Close()
	require.NoError(t, err)

	// check the size before and after closing
	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
	require.Equal(t, width, n)
}

// ------ HELPER FUNCTIONS -------

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(record)
		require.NoError(t, err)
		// verifying the width with the number of bytes returned
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, record, read)
		// moving the position forward to read the next record
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		// get the number of bytes needed to read the whole record
		size := make([]byte, lenWidth)
		n, err := s.ReadAt(size, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)
		// moving the offset forward to read the record
		off += int64(n)

		recordLen := encoding.Uint64(size)
		b := make([]byte, recordLen)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, record, b)
		require.Equal(t, int(recordLen), n)
		off += int64(n)
	}
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		// read and write permisions
		// create if doesn't exist
		// append with each write operation
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		// Owner: 6 - read and write
		// Group: 4 - read
		// Others: 4 - read
		0644,
	)
	if err != nil {
		return nil, 0, err
	}

	fileInfo, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}

	return f, fileInfo.Size(), nil
}
