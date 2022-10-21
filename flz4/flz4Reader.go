package flz4

import (
	"errors"
	"github.com/pierrec/lz4/v4"
	"github.com/xiaojun207/go-base-utils/math"
	"io"
	"log"
	"os"
	"sync"
)

type FLz4 struct {
	Reader     *lz4.Reader
	r          *os.File
	readLock   sync.RWMutex
	readOffset int64
	MaxOffset  int64
	buf        []byte
}

func NewReader(r *os.File) *FLz4 {
	return &FLz4{
		r:      r,
		Reader: lz4.NewReader(r),
	}
}

func (f *FLz4) Read(buf []byte) (n int, err error) {
	return f.Reader.Read(buf)
}

func (f *FLz4) reset() {
	f.r.Seek(0, 0)
	f.Reader.Reset(f.r)
	f.readOffset = 0
}

// ReadAt no buf ReadAt
func (f *FLz4) ReadAt(b []byte, offset int64) (int, error) {
	return f.readAt(b, offset)
	//return f.readAtWithBuf(b, offset)
}

// ReadAt no buf ReadAt
func (f *FLz4) readAt(b []byte, offset int64) (int, error) {
	f.readLock.Lock()
	defer f.readLock.Unlock()

	if f.readOffset > offset {
		// 数据已经读取了，需要重新读取
		f.reset()
		if offset > 0 {
			io.CopyN(io.Discard, f.Reader, offset)
			f.readOffset = offset
		}
	} else if offset > f.readOffset {
		diffLn := offset - f.readOffset
		n, err := io.CopyN(io.Discard, f.Reader, diffLn)

		if err != nil || n < diffLn {
			if err.Error() != "EOF" {
				log.Println("FLz4.readAt.Discard.err:", err, ",readOffset:", f.readOffset, ",offset:", offset, ",n:", n)
			}
			// 数据已经到底了
			f.reset()
			return 0, err
		}
		// 数据还没有读取，也没有到底
		f.readOffset = offset
	} else {
		// f.readOffset == offset
	}

	nn, err := f.Reader.Read(b)
	if err != nil {
		if err.Error() != "EOF" {
			log.Println("FLz4.readAt.Read.err:", err)
		}
		f.reset()
		return 0, err
	}
	f.readOffset = offset + int64(nn)
	f.MaxOffset = math.Max(f.readOffset, f.MaxOffset)
	return nn, err
}

// readAtWithBuf , with buf,
func (f *FLz4) readAtWithBuf(b []byte, offset int64) (n int, err error) {
	f.readLock.Lock()
	defer f.readLock.Unlock()

	ln := int64(len(b))
	bufLn := int64(len(f.buf))

	if bufLn >= offset+ln {
		copy(b, f.buf[offset:offset+ln])
		return int(ln), nil
	}
	diffLn := offset + ln - bufLn

	var tmpBuf = make([]byte, diffLn)
	n, err = f.Reader.Read(tmpBuf)
	if err != nil {
		return
	}
	f.buf = append(f.buf, tmpBuf[:n]...)

	bufLn = int64(len(f.buf))
	if bufLn >= offset+ln {
		copy(b, f.buf[offset:offset+ln])
		return int(ln), nil
	} else if bufLn >= offset {
		copy(b, f.buf[offset:bufLn])
		return int(bufLn - offset), nil
	} else {
		return 0, errors.New("EOF")
	}
}
