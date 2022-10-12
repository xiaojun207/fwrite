package fwrite

import (
	"errors"
	"io"
	"log"
)

func (f *FWriter) readAt(b []byte, offset int64) (int, error) {
	f.readLock.Lock()
	defer f.readLock.Unlock()
	reset := func() {
		f.reader = nil
		f.readOffset = 0
	}

	if f.reader == nil || f.readOffset > offset {
		f.reader = f.GetReader()
		f.readOffset = 0
	}
	if offset-f.readOffset > 0 {
		n, err := io.CopyN(io.Discard, f.reader, offset-f.readOffset)
		if err != nil || n != offset-f.readOffset {
			reset()
			if err.Error() != "EOF" {
				log.Println("readAt.Discard.err:", err, ",readOffset:", f.readOffset, ",offset:", offset, ",n:", n)
			}
			return 0, err
		}
		f.readOffset = offset
	}

	nn, err := f.reader.Read(b)
	if err != nil {
		reset()
		log.Println("readAt.Read.err:", err)
		return 0, err
	}
	f.readOffset = offset + int64(nn)
	return nn, err
}

// index is start at 0
func (f *FWriter) Read(index uint) ([]byte, error) {
	if index >= uint(len(f.offsetList)-1) {
		return nil, errors.New("FWriter read, index is out of range")
	}
	offset := f.offsetList[index]
	offsetNext := f.offsetList[index+1]
	length := offsetNext - offset - LengthSide - HeadSize

	//log.Println("Read,length:", length, ",offset:", offset+LengthSide+HeadSize)
	var b = make([]byte, length)
	c, err := f.readAt(b, offset+LengthSide+HeadSize)
	if int64(c) != length {
		log.Println("FWriter.Read, count:", c, ", length:", length, ", offset:", offset)
	}
	//return UnLz4(b), err
	return b, err
}

func (f *FWriter) FileSize() int64 {
	return Size(f.path)
}
