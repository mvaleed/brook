// Package asyncwriter
package asyncwriter

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"sync"
	"time"
)

var ErrWriteAfterClose = errors.New("write called after writer closed")

type AsyncWriter struct {
	queue    chan *bytes.Buffer
	done     chan struct{}
	writer   *bufio.Writer
	wg       sync.WaitGroup
	flushReq chan chan error
	once     sync.Once
	pool     sync.Pool
}

func NewAsyncWriterSize(w io.Writer, writerBufferSize int) *AsyncWriter {
	aw := &AsyncWriter{
		queue:    make(chan *bytes.Buffer, 10), // Tune buffer size for performance
		done:     make(chan struct{}),
		writer:   bufio.NewWriterSize(w, writerBufferSize),
		flushReq: make(chan chan error),
		pool: sync.Pool{
			New: func() any {
				return bytes.NewBuffer(make([]byte, 0, 4096))
			},
		},
	}
	aw.wg.Add(1)
	go aw.writerLoop()
	return aw
}

func (aw *AsyncWriter) writerLoop() {
	defer aw.wg.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case data := <-aw.queue:
			_, _ = aw.writer.Write(data.Bytes()) // TODO: handle error on write
			aw.pool.Put(data)
		case <-ticker.C:
			aw.writer.Flush()
		case resp := <-aw.flushReq:
			resp <- aw.writer.Flush()
		case <-aw.done:
			aw.onDone()
			return
		}
	}
}

func (aw *AsyncWriter) onDone() {
	for {
		select {
		case data := <-aw.queue:
			_, _ = aw.writer.Write(data.Bytes())
			aw.pool.Put(data)
		case resp := <-aw.flushReq:
			resp <- aw.writer.Flush()
		default:
			aw.writer.Flush()
			return
		}
	}
}

func (aw *AsyncWriter) Write(b []byte) (int, error) {
	poolBuf := aw.pool.Get().(*bytes.Buffer)
	poolBuf.Reset()
	poolBuf.Write(b)

	select {
	case aw.queue <- poolBuf:
		return len(b), nil
	case <-aw.done:
		aw.pool.Put(poolBuf)
		return 0, ErrWriteAfterClose
	}
}

func (aw *AsyncWriter) Flush() error {
	resp := make(chan error, 1)
	select {
	case aw.flushReq <- resp:
		return <-resp
	case <-aw.done:
		return ErrWriteAfterClose
	}
}

func (aw *AsyncWriter) Close() error {
	aw.once.Do(func() {
		close(aw.done)
	})
	aw.wg.Wait()
	return nil
}

var _ io.WriteCloser = (*AsyncWriter)(nil)
