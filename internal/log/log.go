package log

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/akkahshh24/proglog/api/v1"
)

type log struct {
	// RWMutex to grant access to multiple readers or a single writer
	// concurrent read access and exclusive write access
	// useful for frequent reads
	mu            sync.RWMutex
	dir           string
	config        Config
	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*log, error) {
	// set defaults
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}

	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	l := &log{
		dir:    dir,
		config: c,
	}

	return l, l.setup()
}

// Setup with existing segments or bootstrap initial segment
func (l *log) setup() error {
	files, err := os.ReadDir(l.dir)
	if err != nil {
		return err
	}

	// get the base offsets from the file name
	var baseOffsets []uint64
	// dir contains store and index files (eg. 16.store and 16.index)
	// so we need a map to check uniqueness as we need to store only unique elements
	offsetExists := make(map[uint64]bool)
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		if !offsetExists[off] {
			offsetExists[off] = true
			baseOffsets = append(baseOffsets, off)
		}
	}

	// arrange in ascending order
	// useful while reading data
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// create segments with the existing base offsets
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
	}

	// bootstrap the inital segment
	if l.segments == nil {
		if err = l.newSegment(l.config.Segment.InitialOffset); err != nil {
			return err
		}
	}

	return nil
}

// Creates a new segment, appends to the log's segment list
// and makes that segment as active to write new records into
func (l *log) newSegment(baseOffset uint64) error {
	s, err := newSegment(l.dir, baseOffset, l.config)
	if err != nil {
		return err
	}

	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

// Appends a record to the log
func (l *log) Append(record *api.Record) (uint64, error) {
	// * exclusive write lock
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}

	return off, err
}

// Reads the record at the given offset
func (l *log) Read(off uint64) (*api.Record, error) {
	// * read lock
	l.mu.RLock()
	defer l.mu.RUnlock()

	// iterate over the segments and to find the appropriate one
	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off <= segment.nextOffset {
			s = segment
			break
		}
	}

	if s == nil || s.nextOffset <= off {
		// * updated this section to include gRPC status error
		// return nil, fmt.Errorf("offset out of range: %d", off)
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}

	return s.Read(off)
}

// To know the oldest offset stored on the node
// useful in a replicated cluster env
func (l *log) LowestOffset() (uint64, error) {
	// * read lock
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.segments[0] == nil {
		return 0, fmt.Errorf("log is empty")
	}

	return l.segments[0].baseOffset, nil
}

// To know the latest offset stored on the node
// useful in a replicated cluster env
func (l *log) HighestOffset() (uint64, error) {
	// * read lock
	l.mu.RLock()
	defer l.mu.RUnlock()

	// get the last segment's next offset
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}

	return off - 1, nil
}

// Close iterates over the segments and closes them
func (l *log) Close() error {
	// * exclusive write lock
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Remove closes the log and removes its data
func (l *log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}

	return os.RemoveAll(l.dir)
}

// Reset removes the log and then creates a new log
func (l *log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}

	return l.setup()
}
