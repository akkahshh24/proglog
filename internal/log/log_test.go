package log

import (
	"os"
	"testing"

	api "github.com/akkahshh24/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *log){
		"append and read a record successfully": testAppendRead,
		"offset out of range error":             testOutOfRangeErr,
		"initialize with existing segments":     testInitExisting,
	} {
		t.Run(scenario, func(t *testing.T) {
			// a fresh log will be created for every test
			dir, err := os.MkdirTemp("", "log_test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := newLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *log) {
	want := &api.Record{
		Value: []byte("Muscleblaze"),
	}

	off, err := log.Append(want)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	got, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, want.Value, got.Value)
}

func testOutOfRangeErr(t *testing.T, log *log) {
	record, err := log.Read(1)
	require.Error(t, err)
	require.Nil(t, record)
}

// Tests if new log sets itself up with the old data
func testInitExisting(t *testing.T, log *log) {
	record := &api.Record{
		Value: []byte("Optimum Nutrition"),
	}

	for i := 0; i < 3; i++ {
		_, err := log.Append(record)
		require.NoError(t, err)
	}

	// close the log
	require.NoError(t, log.Close())

	// check lowest and highest offset
	off, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	// re-open log
	l, err := newLog(log.dir, log.config)
	require.NoError(t, err)

	// check lowest and highest offset
	off, err = l.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = l.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}
