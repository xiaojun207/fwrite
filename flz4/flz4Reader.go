package flz4

import (
	"github.com/pierrec/lz4/v4"
	"github.com/xiaojun207/fwrite/utils"
	"io"
	"sync"
)

type FLz4 struct {
	*lz4.Writer
	*lz4.Reader
	readSeeker io.ReadSeeker
	readLock   sync.RWMutex
	readOffset int64
	MaxOffset  int64
	firstPos   int64
}

func NewReader(reader io.ReadSeeker, firstPos int64) *FLz4 {
	f := &FLz4{
		readSeeker: reader,
		firstPos:   firstPos,
		Reader:     lz4.NewReader(reader),
	}
	f.Reader.Apply(lz4.ConcurrencyOption(-1))
	return f
}

func (f *FLz4) Read(buf []byte) (n int, err error) {
	n, err = f.Reader.Read(buf)
	if err != nil {
		f.readOffset += int64(n)
	}
	return
}

func (f *FLz4) Reset() {
	f.readSeeker.Seek(f.firstPos, 0)
	f.Reader.Reset(f.readSeeker)
	f.readOffset = 0
}

func (f *FLz4) setReadOffset(offset int64) {
	f.readOffset = offset
	if offset <= f.MaxOffset {
		return
	}
	f.MaxOffset = f.readOffset
}

// ReadAt no buf ReadAt
func (f *FLz4) ReadAt(b []byte, offset int64) (int, error) {
	f.readLock.Lock()
	defer f.readLock.Unlock()
	n, err := f.readAt(b, offset)
	//return f.readAtWithBuf(b, offset)
	return n, err
}

func (f *FLz4) seekAt(offset int64) error {
	if f.readOffset > offset {
		// 数据已经读取了，需要重新读取
		f.Reset()
		if offset > 0 {
			io.CopyN(io.Discard, f.Reader, offset)
			f.readOffset = offset
		}
	} else if offset > f.readOffset {
		diffLn := offset - f.readOffset
		n, err := io.CopyN(io.Discard, f.Reader, diffLn)

		if err != nil || n < diffLn {
			utils.PrintlnError(err, "FLz4.seekAt.Discard.err:", err, ",readOffset:", f.readOffset, ",offset:", offset, ",n:", n)
			// 数据已经到底了
			f.Reset()
			return err
		}
		// 数据还没有读取，也没有到底
		f.readOffset = offset
		f.setReadOffset(f.readOffset)
	} else {
		// f.readOffset == offset
	}
	return nil
}

// ReadAt no buf ReadAt
func (f *FLz4) readAt(b []byte, offset int64) (int, error) {
	err := f.seekAt(offset)

	if err != nil {
		utils.PrintlnError(err, "FLz4.readAt.err:", err, ",readOffset:", f.readOffset, ",offset:", offset)
		return 0, err
	}

	nn, err := f.Reader.Read(b)
	if err != nil {
		utils.PrintlnError(err, "FLz4.readAt.Read.err:", err)
		f.Reset()
		return 0, err
	}
	f.setReadOffset(offset + int64(nn))
	return nn, err
}

func (f *FLz4) ReadSeek(offset, size int64, receiver Receiver) (int64, error) {
	err := f.seekAt(offset)
	if err != nil {
		utils.PrintlnError(err, "FLz4.readAt.err:", err, ",readOffset:", f.readOffset, ",offset:", offset)
		return 0, err
	}

	nn, err := io.CopyN(receiver, f.Reader, size)
	if err != nil {
		utils.PrintlnError(err, "FLz4.readAt.Read.err:", err)
		f.Reset()
		return 0, err
	}
	f.setReadOffset(offset + int64(nn))
	return nn, err
}

func (f *FLz4) WriteTo(w io.Writer) (n int64, err error) {
	n, err = f.Reader.WriteTo(w)
	if err != nil {
		f.readOffset += int64(n)
	}
	return
}
