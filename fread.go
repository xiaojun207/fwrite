package fwrite

import (
	"errors"
	"fmt"
	"github.com/xiaojun207/fwrite/flz4"
	"io"
	"log"
	"os"
)

type FReader struct {
	path   string
	reader IOReader
}

func (f *FReader) GetReader() (reader IOReader) {
	if !exists(f.path) {
		// 创建空文件
		os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	}
	file, err := os.OpenFile(f.path, os.O_RDONLY, 0)
	if err != nil {
		log.Fatalln("FWriter.GetReader, 文件打开失败", err)
	}
	reader = flz4.NewReader(file)
	return
}

func (f *FReader) readAt(b []byte, offset int64) (int, error) {
	if f.reader == nil {
		f.reader = f.GetReader()
	}
	return f.reader.ReadAt(b, offset)
}

func (f *FReader) Test() {
	offset := int64(0)
	var d = make([]byte, 6)
	n, err := f.readAt(d, offset)
	log.Println("err:", err, ",n:", n, ",d:", d)
}

func (f *FReader) foreach(offset int64, query func(idx uint64, offset int64, length LenInt, d []byte) bool) uint64 {
	idx := uint64(0)
	length := LenInt(0)
	for true {
		var d = make([]byte, LengthSide)
		_, err := f.readAt(d, offset+HeadSize)
		if err != nil {
			PrintlnError(err, "FReader.foreach err:", err, ",idx:", idx, ",offset:", offset)
			break
		}
		length = toLenInt(d)

		var b = make([]byte, length)
		_, err = f.readAt(b, offset+HeadSize+LengthSide)
		if err != nil {
			PrintlnError(err, "FReader.foreach err:", err, ",idx:", idx, ",offset:", offset)
			break
		}
		if !query(idx, offset, length, b) {
			break
		}
		offset += HeadSize + LengthSide + int64(length)
		idx++
	}
	return idx
}

func (f *FReader) foreach2(offset int64, query func(idx uint64, offset int64, length LenInt, d []byte) bool) (idx uint64, err error) {
	length := LenInt(0)
	reader := f.GetReader()
	_, err = io.CopyN(io.Discard, reader, offset)
	for true {
		_, err = io.CopyN(io.Discard, reader, HeadSize)
		if err != nil {
			if err.Error() != "EOF" {
				return
			}
			break
		}
		var ln = make([]byte, LengthSide)
		_, err = reader.Read(ln)
		if err != nil {
			PrintlnError(err, "FReader.foreach2 err:", err, ",idx:", idx, ",offset:", offset)
			break
		}
		length = toLenInt(ln)

		var b = make([]byte, length)
		_, err = reader.Read(b)

		if err != nil {
			break
		}
		if !query(idx, offset, length, b) {
			break
		}
		offset += HeadSize + LengthSide + int64(length)
		idx++
	}
	return
}

// Foreach reset reader read all
func (f *FReader) Foreach(filter func(idx uint64, offset int64, length LenInt, d []byte) bool) (idx uint64, err error) {
	length := LenInt(0)
	offset := int64(0)
	reader := f.GetReader()
	_, err = io.CopyN(io.Discard, reader, offset)
	if err != nil {
		if err.Error() != "EOF" {
			return idx, err
		}
		return idx, err
	}
	for true {
		_, err = io.CopyN(io.Discard, reader, HeadSize)
		if err != nil {
			if err.Error() != "EOF" {
				return idx, err
			}
			break
		}
		var ln = make([]byte, LengthSide)
		_, err = reader.Read(ln)
		length = toLenInt(ln)

		var b = make([]byte, length)
		_, err = reader.Read(b)

		if err != nil {
			if err.Error() != "EOF" {
				return idx, err
			}
			break
		}
		if !filter(idx, offset, length, b) {
			return
		}
		offset += HeadSize + LengthSide + int64(length)
		idx++
	}
	return idx, nil
}

func (f *FReader) ForEach2(filter func(d []byte) bool) (err error) {
	f.foreach2(0, func(idx uint64, offset int64, length LenInt, d []byte) bool {
		return filter(d)
	})
	return nil
}

func (f *FReader) ForEach(filter func(d []byte) bool) (err error) {
	f.foreach(0, func(idx uint64, offset int64, length LenInt, d []byte) bool {
		return filter(d)
	})
	return nil
}

// read , depend on idx
func (f *FWriter) read(index int) ([]byte, error) {
	offset, length := f.FIdx.getOffset(index)
	var b = make([]byte, length)
	_, err := f.FReader.readAt(b, int64(offset+LengthSide+HeadSize))
	return b, err
}

// index is start at 0,  depend on idx
func (f *FWriter) Read(index uint64) ([]byte, error) {
	f.loadIdx()
	if index >= f.FIdx.getIdxNum() {
		return nil, errors.New(fmt.Sprint("FWriter read, index is out of range, index:", index, ",idxNum:", f.FIdx.getIdxNum()))
	}
	return f.read(int(index))
}

// Search ,  depend on idx
func (f *FWriter) Search(filter func(d []byte) bool) (res [][]byte, err error) {
	f.foreach(0, func(idx uint64, offset int64, length LenInt, d []byte) bool {
		if filter(d) {
			res = append(res, d)
		}
		return true
	})
	return res, nil
}
