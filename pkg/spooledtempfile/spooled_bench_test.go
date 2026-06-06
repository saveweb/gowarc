package spooledtempfile

import (
	"bytes"
	"crypto/sha256"
	"io"
	"math/rand"
	"path/filepath"
	"testing"
)

func BenchmarkWrite(b *testing.B) {
	const total = 4 << 20
	const chunk = 128 << 10
	data := randBytes(chunk)
	tempDir := makeTempDir(&testing.T{})

	b.ReportAllocs()
	b.SetBytes(total)
	for b.Loop() {
		s, err := NewSpooledTempFile("bench", tempDir)
		if err != nil {
			b.Fatal(err)
		}
		repeatWrite(b, s, total, chunk, data)
		if err := s.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRead_AfterWrite(b *testing.B) {
	const total = 8 << 20
	const chunk = 128 << 10
	data := randBytes(chunk)
	tempDir := makeTempDir(&testing.T{})

	s, err := NewSpooledTempFile("bench", tempDir)
	if err != nil {
		b.Fatal(err)
	}
	repeatWrite(b, s, total, chunk, data)

	buf := make([]byte, 128<<10)
	b.ReportAllocs()
	b.SetBytes(int64(total))
	b.ResetTimer()
	for b.Loop() {
		if _, err := s.Seek(0, io.SeekStart); err != nil {
			b.Fatal(err)
		}
		read := 0
		for {
			n, err := s.Read(buf)
			if n > 0 {
				read += n
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	if err := s.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkReadAt(b *testing.B) {
	const total = 16 << 20
	const block = 64 << 10
	tempDir := makeTempDir(&testing.T{})
	payload := bytes.Repeat([]byte{0xAB}, 256<<10)

	s, err := NewSpooledTempFile("bench", tempDir)
	if err != nil {
		b.Fatal(err)
	}
	repeatWrite(b, s, total, len(payload), payload)

	buf := make([]byte, block)
	b.ReportAllocs()
	b.SetBytes(block)
	b.ResetTimer()
	pos := int64(0)
	for b.Loop() {
		if pos >= total-int64(block) {
			pos = 0
		}
		if _, err := s.ReadAt(buf, pos); err != nil && err != io.EOF {
			b.Fatal(err)
		}
		pos += int64(block)
	}
	b.StopTimer()
	if err := s.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkParallel_CreateAndWrite(b *testing.B) {
	const total = 512 << 10
	const chunk = 32 << 10
	data := randBytes(chunk)
	tempDir := makeTempDir(&testing.T{})

	b.ReportAllocs()
	b.SetBytes(total)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s, err := NewSpooledTempFile("bench", tempDir)
			if err != nil {
				b.Fatal(err)
			}
			repeatWrite(b, s, total, chunk, data)
			if err := s.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRead_Hash(b *testing.B) {
	const total = 32 << 20
	const chunk = 256 << 10
	data := randBytes(chunk)
	tempDir := makeTempDir(&testing.T{})

	s, err := NewSpooledTempFile("bench", tempDir)
	if err != nil {
		b.Fatal(err)
	}
	repeatWrite(b, s, total, chunk, data)

	buf := make([]byte, 256<<10)
	b.ReportAllocs()
	b.SetBytes(int64(total))
	b.ResetTimer()
	for b.Loop() {
		h := sha256.New()
		if _, err := s.Seek(0, io.SeekStart); err != nil {
			b.Fatal(err)
		}
		for {
			n, err := s.Read(buf)
			if n > 0 {
				_, _ = h.Write(buf[:n])
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
		_ = h.Sum(nil)
	}
	b.StopTimer()
	if err := s.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkCreateClose_NoWrite(b *testing.B) {
	tempDir := makeTempDir(&testing.T{})
	b.ReportAllocs()
	for b.Loop() {
		s, err := NewSpooledTempFile("bench", tempDir)
		if err != nil {
			b.Fatal(err)
		}
		if err := s.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func randBytes(n int) []byte {
	r := rand.New(rand.NewSource(42))
	b := make([]byte, n)
	_, _ = r.Read(b)
	return b
}

func repeatWrite(t *testing.B, w io.Writer, total, chunk int, payload []byte) {
	written := 0
	for written < total {
		to := chunk
		if written+to > total {
			to = total - written
		}
		if _, err := w.Write(payload[:to]); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		written += to
	}
}

func makeTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	return abs
}
