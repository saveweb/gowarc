package warc

import (
	"compress/zlib"
	"errors"
	"io"
	"strings"

	gzip "github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
)

type readerAndCloser struct {
	io.Reader
	io.Closer
}

type multiCloser struct {
	closers []io.Closer
}

func newMultiCloser(closers ...io.Closer) *multiCloser {
	return &multiCloser{closers: closers}
}

func (m *multiCloser) Close() error {
	var err error
	for _, closer := range m.closers {
		if e := closer.Close(); e != nil {
			err = errors.Join(err, e)
		}
	}
	return err
}

func decompressBody(contentEncoding string, body io.ReadCloser) (io.ReadCloser, error) {
	switch strings.ToLower(contentEncoding) {
	case "gzip":
		gzReader, err := gzip.NewReader(body)
		if err != nil {
			body.Close()
			return nil, err
		}
		return &readerAndCloser{
			Reader: gzReader,
			Closer: newMultiCloser(body, gzReader),
		}, nil
	case "deflate":
		zlibReader, err := zlib.NewReader(body)
		if err != nil {
			body.Close()
			return nil, err
		}
		return &readerAndCloser{
			Reader: zlibReader,
			Closer: newMultiCloser(body, zlibReader),
		}, nil
	case "zstd":
		zstdReader, err := zstd.NewReader(body)
		if err != nil {
			body.Close()
			return nil, err
		}
		zstdReaderCloser := zstdReader.IOReadCloser()
		return &readerAndCloser{
			Reader: zstdReaderCloser,
			Closer: newMultiCloser(body, zstdReaderCloser),
		}, nil
	}
	return body, nil
}
