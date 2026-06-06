package spooledtempfile

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func generateTestDataInKB(size int) []byte {
	return bytes.Repeat([]byte("A"), size*1024)
}

func TestBasicWriteAndRead(t *testing.T) {
	spool, err := NewSpooledTempFile("test", os.TempDir())
	if err != nil {
		t.Fatalf("NewSpooledTempFile error: %v", err)
	}
	defer spool.Close()

	input := []byte("hello, world")
	n, err := spool.Write(input)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if n != len(input) {
		t.Errorf("Write count mismatch: got %d, want %d", n, len(input))
	}

	if spool.Len() != int64(len(input)) {
		t.Errorf("Len() mismatch: got %d, want %d", spool.Len(), len(input))
	}

	if spool.Name() == "" {
		t.Fatal("Expected a file name, got empty")
	}

	out := make([]byte, 5)
	n, err = spool.Read(out)
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}
	if n != 5 {
		t.Errorf("Read count mismatch: got %d, want 5", n)
	}
	if string(out) != "hello" {
		t.Errorf(`Data mismatch: got "%s", want "hello"`, string(out))
	}

	out2 := make([]byte, 20)
	n, err = spool.Read(out2)
	expectedRemainder := "o, world"[1:]
	if err != io.EOF && err != nil {
		t.Fatalf("Expected EOF or nil error, got: %v", err)
	}
	if string(out2[:n]) != expectedRemainder {
		t.Errorf(`Data mismatch: got "%s", want "%s"`, string(out2[:n]), expectedRemainder)
	}
}

func TestLargeWrite(t *testing.T) {
	spool, err := NewSpooledTempFile("test", os.TempDir())
	if err != nil {
		t.Fatalf("NewSpooledTempFile error: %v", err)
	}
	defer spool.Close()

	data := generateTestDataInKB(500)
	_, err = spool.Write(data)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if spool.Len() != int64(len(data)) {
		t.Errorf("Len() mismatch: got %d, want %d", spool.Len(), len(data))
	}

	if spool.Name() == "" {
		t.Fatal("Expected a file name, got empty")
	}

	out, err := io.ReadAll(spool)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if !bytes.Equal(data, out) {
		t.Errorf("Data mismatch. Got %q, want %q", out, data)
	}
}

func TestReadAtAndSeek(t *testing.T) {
	spool, err := NewSpooledTempFile("test", os.TempDir())
	if err != nil {
		t.Fatalf("NewSpooledTempFile error: %v", err)
	}
	defer spool.Close()

	data := []byte("HelloWorld123")
	_, err = spool.Write(data)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}

	_, err = spool.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek error: %v", err)
	}

	p := make([]byte, 5)
	n, err := spool.ReadAt(p, 5)
	if err != nil {
		t.Fatalf("ReadAt error: %v", err)
	}
	if n != 5 {
		t.Errorf("ReadAt count mismatch: got %d, want 5", n)
	}
	if string(p) != "World" {
		t.Errorf(`Data mismatch: got "%s", want "World"`, string(p))
	}

	_, err = spool.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek error: %v", err)
	}
	all, err := io.ReadAll(spool)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if !bytes.Equal(data, all) {
		t.Errorf("Data mismatch. Got %q, want %q", all, data)
	}
}

func TestWriteAfterReadPanic(t *testing.T) {
	spool, err := NewSpooledTempFile("test", os.TempDir())
	if err != nil {
		t.Fatalf("NewSpooledTempFile error: %v", err)
	}
	defer spool.Close()

	_, err = spool.Write([]byte("ABCDEFG"))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}

	buf := make([]byte, 4)
	_, err = spool.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read error: %v", err)
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("Expected panic on write after read, got none")
		} else {
			msg := fmt.Sprintf("%v", r)
			if !strings.Contains(msg, "write after read") {
				t.Errorf(`Expected panic message "write after read", got %q`, msg)
			}
		}
	}()
	_, _ = spool.Write([]byte("XYZ"))
	t.Fatal("We should not reach here, expected panic")
}

func TestClose(t *testing.T) {
	spool, err := NewSpooledTempFile("test", os.TempDir())
	if err != nil {
		t.Fatalf("NewSpooledTempFile error: %v", err)
	}

	_, err = spool.Write([]byte("Small data"))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}

	fn := spool.Name()
	if fn == "" {
		t.Fatal("Expected a file name, got empty")
	}
	if _, statErr := os.Stat(fn); statErr != nil {
		t.Fatalf("Expected file to exist, got error: %v", statErr)
	}

	err = spool.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}

	_, statErr := os.Stat(fn)
	if !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("Expected file to be removed on Close, stat returned: %v", statErr)
	}

	_, err = spool.Read(make([]byte, 10))
	if err != io.EOF {
		t.Errorf("Expected EOF after close, got %v", err)
	}

	_, err = spool.Write([]byte("More data"))
	if err != io.EOF {
		t.Errorf("Expected io.EOF after close on write, got %v", err)
	}
}

func TestLen(t *testing.T) {
	spool, err := NewSpooledTempFile("test", os.TempDir())
	if err != nil {
		t.Fatalf("NewSpooledTempFile error: %v", err)
	}
	defer spool.Close()

	data := []byte("123456789")
	_, err = spool.Write(data)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if spool.Len() != 9 {
		t.Errorf("Len() mismatch: got %d, want 9", spool.Len())
	}
}

func TestFileName(t *testing.T) {
	spool, err := NewSpooledTempFile("testprefix", os.TempDir())
	if err != nil {
		t.Fatalf("NewSpooledTempFile error: %v", err)
	}
	defer spool.Close()

	_, err = spool.Write([]byte("data"))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}

	fn := spool.Name()
	if fn == "" {
		t.Fatal("Expected FileName, got empty")
	}

	base := filepath.Base(fn)
	if !strings.HasPrefix(base, "testprefix") {
		t.Errorf("Expected file name prefix 'testprefix', got %s", base)
	}
}

func TestSpoolingLargeData(t *testing.T) {
	spool, err := NewSpooledTempFile("test", os.TempDir())
	if err != nil {
		t.Fatalf("NewSpooledTempFile error: %v", err)
	}
	defer spool.Close()

	data := generateTestDataInKB(500)
	_, err = io.Copy(spool, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}

	if spool.Len() != 500*1024 {
		t.Fatalf("Data length mismatch: got %d, want %d", spool.Len(), 500*1024)
	}
	if spool.Name() == "" {
		t.Error("Expected a file name, got empty")
	}

	out := make([]byte, len(data))
	_, err = spool.ReadAt(out, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt error: %v", err)
	}
	if !bytes.Equal(out, data) {
		t.Errorf("Data mismatch. Got %q, want %q", out, data)
	}
}
