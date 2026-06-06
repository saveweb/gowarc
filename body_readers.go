package warc

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type chunkedBodyReader struct {
	br             *bufio.Reader
	warcWriter     io.Writer
	decoded        bytes.Buffer
	done           bool
	n              int64
	conn           net.Conn
	readDeadline   time.Duration
}

func newChunkedBodyReader(br *bufio.Reader, warcWriter io.Writer) *chunkedBodyReader {
	return &chunkedBodyReader{
		br:         br,
		warcWriter: warcWriter,
	}
}

func (r *chunkedBodyReader) setReadDeadline() {
	if r.conn != nil && r.readDeadline > 0 {
		r.conn.SetReadDeadline(time.Now().Add(r.readDeadline))
	}
}

func (r *chunkedBodyReader) Read(p []byte) (int, error) {
	for r.decoded.Len() == 0 && !r.done {
		r.setReadDeadline()
		sizeLine, err := r.br.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return 0, io.ErrUnexpectedEOF
			}
			return 0, fmt.Errorf("chunkedBodyReader: reading chunk size: %w", err)
		}

		if r.warcWriter != nil {
			if _, werr := io.WriteString(r.warcWriter, sizeLine); werr != nil {
				return 0, fmt.Errorf("chunkedBodyReader: writing chunk size to warc: %w", werr)
			}
		}

		trimmed := strings.TrimSpace(sizeLine)
		if semi := strings.Index(trimmed, ";"); semi >= 0 {
			trimmed = trimmed[:semi]
		}
		chunkSize, err := strconv.ParseInt(trimmed, 16, 64)
		if err != nil {
			return 0, fmt.Errorf("chunkedBodyReader: parsing chunk size %q: %w", trimmed, err)
		}

		if chunkSize == 0 {
			for {
				line, err := r.br.ReadString('\n')
				if err != nil && !errors.Is(err, io.EOF) {
					return 0, fmt.Errorf("chunkedBodyReader: reading trailer: %w", err)
				}
				if r.warcWriter != nil {
					if _, werr := io.WriteString(r.warcWriter, line); werr != nil {
						return 0, fmt.Errorf("chunkedBodyReader: writing trailer to warc: %w", werr)
					}
				}
				if line == "\r\n" || line == "\n" || errors.Is(err, io.EOF) {
					break
				}
			}
			r.done = true
			break
		}

		data := make([]byte, chunkSize)
		if _, err := io.ReadFull(r.br, data); err != nil {
			return 0, fmt.Errorf("chunkedBodyReader: reading chunk data: %w", err)
		}

		if r.warcWriter != nil {
			if _, werr := r.warcWriter.Write(data); werr != nil {
				return 0, fmt.Errorf("chunkedBodyReader: writing chunk data to warc: %w", werr)
			}
		}

		r.decoded.Write(data)
		r.n += chunkSize

		crlf := make([]byte, 2)
		if _, err := io.ReadFull(r.br, crlf); err != nil {
			return 0, fmt.Errorf("chunkedBodyReader: reading chunk CRLF: %w", err)
		}
		if r.warcWriter != nil {
			if _, werr := r.warcWriter.Write(crlf); werr != nil {
				return 0, fmt.Errorf("chunkedBodyReader: writing chunk CRLF to warc: %w", werr)
			}
		}
	}

	return r.decoded.Read(p)
}

func (r *chunkedBodyReader) BytesRead() int64 {
	return r.n
}

type limitedBodyReader struct {
	br           *bufio.Reader
	warcWriter   io.Writer
	remaining    int64
	conn         net.Conn
	readDeadline time.Duration
}

func newLimitedBodyReader(br *bufio.Reader, warcWriter io.Writer, contentLength int64) *limitedBodyReader {
	return &limitedBodyReader{
		br:         br,
		warcWriter: warcWriter,
		remaining:  contentLength,
	}
}

func (r *limitedBodyReader) setReadDeadline() {
	if r.conn != nil && r.readDeadline > 0 {
		r.conn.SetReadDeadline(time.Now().Add(r.readDeadline))
	}
}

func (r *limitedBodyReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	n := len(p)
	if int64(n) > r.remaining {
		n = int(r.remaining)
	}
	r.setReadDeadline()
	m, err := r.br.Read(p[:n])
	r.remaining -= int64(m)
	if r.warcWriter != nil {
		r.warcWriter.Write(p[:m])
	}
	return m, err
}

type eofBodyReader struct {
	br           *bufio.Reader
	warcWriter   io.Writer
	eof          bool
	conn         net.Conn
	readDeadline time.Duration
}

func newEOFBodyReader(br *bufio.Reader, warcWriter io.Writer) *eofBodyReader {
	return &eofBodyReader{
		br:         br,
		warcWriter: warcWriter,
	}
}

func (r *eofBodyReader) setReadDeadline() {
	if r.conn != nil && r.readDeadline > 0 {
		r.conn.SetReadDeadline(time.Now().Add(r.readDeadline))
	}
}

func (r *eofBodyReader) Read(p []byte) (int, error) {
	if r.eof {
		return 0, io.EOF
	}
	r.setReadDeadline()
	n, err := r.br.Read(p)
	if r.warcWriter != nil {
		r.warcWriter.Write(p[:n])
	}
	if err == io.EOF {
		r.eof = true
	}
	return n, err
}
