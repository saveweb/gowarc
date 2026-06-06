package spooledtempfile

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// ReaderAt is the interface for ReadAt - read at position, without moving pointer.
type ReaderAt interface {
	ReadAt(p []byte, off int64) (n int, err error)
}

// ReadSeekCloser is an io.Reader + ReaderAt + io.Seeker + io.Closer + Stat
type ReadSeekCloser interface {
	io.ReadSeekCloser
	ReaderAt
	Name() string
	Len() int64
}

// spooledTempFile writes to a temporary file and deletes it on Close.
type spooledTempFile struct {
	file       *os.File
	filePrefix string
	tempDir    string
	reading    bool // transitions at most once from false -> true
	closed     bool
}

// ReadWriteSeekCloser is an io.Writer + io.Reader + io.Seeker + io.Closer.
type ReadWriteSeekCloser interface {
	ReadSeekCloser
	io.Writer
}

// NewSpooledTempFile returns an ReadWriteSeekCloser,
// with some important constraints:
//   - You can Write into it, but whenever you call Read or Seek on it,
//     subsequent Write calls will panic.
func NewSpooledTempFile(filePrefix string, tempDir string) (ReadWriteSeekCloser, error) {
	file, err := os.CreateTemp(tempDir, filePrefix+"-")
	if err != nil {
		return nil, fmt.Errorf("spooledtempfile: creating temp file: %w", err)
	}
	return &spooledTempFile{
		file:       file,
		filePrefix: filePrefix,
		tempDir:    tempDir,
	}, nil
}

func (s *spooledTempFile) prepareRead() error {
	if s.closed {
		return io.EOF
	}

	if s.reading {
		return nil
	}

	s.reading = true
	if _, err := s.file.Seek(0, 0); err != nil {
		return fmt.Errorf("file=%v: %w", s.file, err)
	}
	return nil
}

func (s *spooledTempFile) Len() int64 {
	fi, err := s.file.Stat()
	if err != nil {
		return -1
	}
	return fi.Size()
}

func (s *spooledTempFile) Read(p []byte) (n int, err error) {
	if err := s.prepareRead(); err != nil {
		return 0, err
	}
	return s.file.Read(p)
}

func (s *spooledTempFile) ReadAt(p []byte, off int64) (n int, err error) {
	if err := s.prepareRead(); err != nil {
		return 0, err
	}
	return s.file.ReadAt(p, off)
}

func (s *spooledTempFile) Seek(offset int64, whence int) (int64, error) {
	if err := s.prepareRead(); err != nil {
		return 0, err
	}
	return s.file.Seek(offset, whence)
}

func (s *spooledTempFile) Write(p []byte) (n int, err error) {
	if s.closed {
		return 0, io.EOF
	}

	if s.reading {
		panic("write after read")
	}

	return s.file.Write(p)
}

func (s *spooledTempFile) Close() error {
	s.closed = true

	if s.file == nil {
		return nil
	}

	s.file.Close()

	if err := os.Remove(s.file.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	s.file = nil
	return nil
}

func (s *spooledTempFile) Name() string {
	return s.file.Name()
}
