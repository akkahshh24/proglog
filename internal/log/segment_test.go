package log

import (
	"io"
	"os"
	"testing"

	api "github.com/akkahshh24/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment_test")
	defer os.RemoveAll(dir)

	want := &api.Record{
		Value: []byte("Khiladi 786"),
	}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entireWidth * 3 // max 3 index entries

	// new segment with base offset 16
	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := 0; i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, uint64(16+i), off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	// error while trying to append more than 3 records
	_, err = s.Append(want)
	require.Error(t, io.EOF, err)
	require.True(t, s.IsMaxed()) // maxed index

	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3) // max 3 records
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.True(t, s.IsMaxed()) // maxed store

	// close and load from exisitng state
	err = s.Close()
	require.NoError(t, err)
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	got, err := s.Read(18)
	require.NoError(t, err)
	require.Equal(t, want.Value, got.Value)

	// clear segment and start fresh
	err = s.Remove()
	require.NoError(t, err)
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
