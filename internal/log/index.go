package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// index contains two fields
// offset: stored as uint32 (4 bytes)
// position: stored as uint64 (8 bytes)
var (
	offsetWidth   uint64 = 4
	positionWidth uint64 = 8
	entireWidth          = offsetWidth + positionWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// Creates an index for the given file
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fileInfo, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fileInfo.Size())

	// extend the file length to MaxIndentBytes before
	// memory-mapping the file as it's not possible after that
	err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes))
	if err != nil {
		return nil, err
	}

	idx.mmap, err = gommap.Map(
		// file descriptor to perform operations on the file
		idx.file.Fd(),
		// allow read and write to the memory-mapped file
		gommap.PROT_READ|gommap.PROT_WRITE,
		// modifications made will be reflected in the file
		gommap.MAP_SHARED,
	)
	if err != nil {
		return nil, err
	}

	return idx, nil
}

// Read takes the offset and returns the associated record's position in the store
// The given offset is relative to the segment's base offset to reduce the size of the offset stored in the index file (uint32 vs uint64)
func (i *index) Read(input int64) (offset uint32, position uint64, err error) {
	// index file is empty
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	// offset of previous record
	// Eg: For 3 entries,
	// size = 36 bytes, entireWidth = 12
	// offset = (36/12) - 1 = 2
	if input == -1 {
		offset = uint32((i.size / entireWidth) - 1)
	} else {
		offset = uint32(input)
	}

	// 1st record info will start from 0 * 12 pos i.e. byte[0] - byte[11]
	// 2nd record info will start from 1 * 12 pos i.e. byte[12] - byte[23]
	// this position refers to the position of the entry in the INDEX file
	position = uint64(offset) * entireWidth
	if i.size < position+entireWidth {
		return 0, 0, io.EOF
	}

	// decode offset and position from the encoded byte data stored in file
	// offset info stored in first 4 bytes
	offset = encoding.Uint32(i.mmap[position : position+offsetWidth])
	// position info stored in next 8 bytes
	// this position refers to the position of the record in the STORE file
	position = encoding.Uint64(i.mmap[position+offsetWidth : position+entireWidth])
	return offset, position, nil
}

// Write appends the given offset and position in the index
func (i *index) Write(off uint32, pos uint64) error {
	// check if space is available for a new entry
	if uint64(len(i.mmap)) < i.size+entireWidth {
		return io.EOF
	}

	// insert offset
	encoding.PutUint32(i.mmap[i.size:i.size+offsetWidth], off)
	// insert position
	encoding.PutUint64(i.mmap[i.size+offsetWidth:i.size+entireWidth], pos)

	// update the index file size
	i.size += entireWidth
	return nil
}

// Close gracefully shuts down the index file
func (i *index) Close() error {
	// flush memory-mapped file changes to the underlying persisted file synchronously
	// strong data consistency
	// requires more time
	err := i.mmap.Sync(gommap.MS_SYNC)
	if err != nil {
		return err
	}

	// flush persisted file to stable storage
	err = i.file.Sync()
	if err != nil {
		return err
	}

	// truncate the file to the amount of data in it
	// to read the last entry (12 bytes) easily
	// when the service restarts
	err = i.file.Truncate(int64(i.size))
	if err != nil {
		return err
	}

	return i.file.Close()
}

func (i *index) Name() string {
	return i.file.Name()
}
