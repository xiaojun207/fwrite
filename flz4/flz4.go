package flz4

import (
	"errors"
	"github.com/pierrec/lz4/v4"
	"io"
	"log"
	"os"
)

type Reset interface {
	Reset()
}

type Receiver struct {
	OnRead func(b []byte)
}

type ReadSeek interface {
	ReadSeek(offset, size int64, receiver Receiver) (int64, error)
}

func (r Receiver) Write(p []byte) (int, error) {
	if r.OnRead != nil {
		r.OnRead(p)
	}
	return len(p), nil
}

func WriteToFile(fileName string, data []byte) {
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Println("FLz4.WriteToFile.err:", err)
		return
	}
	writer := lz4.NewWriter(file)
	writer.Write(data)
	writer.Flush()
}

func ReadFromFile(fileName string) ([]byte, error) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Println("FLz4.ReadFromFile.err:", err)
		return nil, err
	}
	r := lz4.NewReader(file)
	return io.ReadAll(r)
}

func exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return !os.IsNotExist(err)
}

func Copy(dst io.Writer, src io.Reader) (written int64, err error) {
	var buf []byte = nil
	if wt, ok := src.(io.WriterTo); ok {
		return wt.WriteTo(dst)
	}
	if rt, ok := dst.(io.ReaderFrom); ok {
		return rt.ReadFrom(src)
	}
	if buf == nil {
		size := 32 * 1024
		if l, ok := src.(*io.LimitedReader); ok && int64(size) > l.N {
			if l.N < 1 {
				size = 1
			} else {
				size = int(l.N)
			}
		}
		buf = make([]byte, size)
	}
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errors.New("invalid write result")
				}
			}
			written += int64(nw)
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}
