package log

/*
 Wrapper around a file with two APIs to append and read bytes
 to and from the file.

 Read https://docs.microsoft.com/en-us/azure/architecture/patterns/event-sourcing for info on append on logs for
 events sourcing.

 Read https://www.confluent.io/en-gb/blog/event-sourcing-cqrs-stream-processing-apache-kafka-whats-connection/ for
 more info on CQRS

*/
import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	encoding = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	// Pointer to the file we are editing
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// Establish a new store, we can use this to initialise the store in another file
func newStore(f *os.File) (*store, error) {
	file, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(file.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Function for the store struct that we can use when we init a new store with the newStore function
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	// Lock the file before use for async purposes
	s.mu.Lock()
	defer s.mu.Unlock()
	// Write the length of the log to file
	pos = s.size
	// Append to the file using the file buffer
	if err := binary.Write(s.buf, encoding, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}
