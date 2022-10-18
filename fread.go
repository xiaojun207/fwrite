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
	f.LoadIndex()

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

func (f *FWriter) Search(query func(d []byte) bool) (res [][]byte, err error) {
	f.LoadIndex()

	count := f.Count()
	index := 0
	for index < count {
		offset := f.offsetList[index]
		offsetNext := f.offsetList[index+1]
		length := offsetNext - offset - LengthSide - HeadSize

		var b = make([]byte, length)
		_, err = f.readAt(b, offset+LengthSide+HeadSize)
		if err != nil {
			if err.Error() != "EOF" {
				return res, err
			}
			break
		}
		if query(b) {
			res = append(res, b)
		}
		index++
	}

	return res, nil
}

// Foreach reset reader read all
func (f *FWriter) Foreach(filter func(d []byte) bool) (err error) {
	index := 0
	reader := f.GetReader()
	for true {
		io.CopyN(io.Discard, reader, HeadSize)

		var ln = make([]byte, LengthSide)
		_, err = reader.Read(ln)
		length := f.toLenInt(ln)

		var b = make([]byte, length)
		_, err = reader.Read(b)

		if err != nil {
			if err.Error() != "EOF" {
				return err
			}
			break
		}
		if !filter(b) {
			return
		}
		index++
	}

	return nil
}

func (f *FWriter) ForEach(filter func(d []byte) bool) (err error) {
	index := 0
	offset := int64(0)
	for true {
		var ln = make([]byte, LengthSide)
		offset += HeadSize
		_, err = f.readAt(ln, offset)
		length := f.toLenInt(ln)

		var b = make([]byte, length)
		offset += LengthSide
		_, err = f.readAt(b, offset)

		if err != nil {
			if err.Error() != "EOF" {
				return err
			}
			break
		}
		if !filter(b) {
			return
		}
		index++
		offset += int64(length)
	}

	return nil
}

func (f *FWriter) FileSize() int64 {
	return Size(f.path)
}
