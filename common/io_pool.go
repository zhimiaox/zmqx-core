package common

import (
	"bufio"
	"io"
	"sync"
)

var (
	ioPoolInstance IOPool
	ioPoolOnce     sync.Once
)

type IOPool interface {
	GetReader(r io.Reader, size int) *bufio.Reader
	PutReader(br *bufio.Reader)
	GetWriter(r io.Writer, size int) *bufio.Writer
	PutWriter(bw *bufio.Writer)
}

type poolImpl struct {
	reader sync.Pool
	writer sync.Pool
}

func (p *poolImpl) GetReader(r io.Reader, size int) *bufio.Reader {
	if v := p.reader.Get(); v != nil {
		br := v.(*bufio.Reader)
		br.Reset(r)
		return br
	}
	return bufio.NewReaderSize(r, size)
}

func (p *poolImpl) PutReader(br *bufio.Reader) {
	br.Reset(nil)
	p.reader.Put(br)
}

func (p *poolImpl) GetWriter(w io.Writer, size int) *bufio.Writer {
	if v := p.writer.Get(); v != nil {
		bw := v.(*bufio.Writer)
		bw.Reset(w)
		return bw
	}
	return bufio.NewWriterSize(w, size)
}

func (p *poolImpl) PutWriter(bw *bufio.Writer) {
	bw.Reset(nil)
	p.writer.Put(bw)
}

func GetIOPool() IOPool {
	ioPoolOnce.Do(func() {
		ioPoolInstance = &poolImpl{}
	})
	return ioPoolInstance
}
