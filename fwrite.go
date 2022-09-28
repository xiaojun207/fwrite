package fwrite

import (
	"bufio"
	"encoding/binary"
	"log"
	"os"
	"sync"
)

type FWriter struct {
	path       string
	idxPath    string
	reader     *os.File
	file       *os.File
	offsetList []int64
	bufWriter  *bufio.Writer
	mutex      sync.RWMutex
	fHeader    []byte
}

func New(path string) *FWriter {
	os.MkdirAll(path, os.ModePerm)
	f := &FWriter{
		path:       path + "/00000001.f",
		idxPath:    path + "/00000001.i",
		offsetList: []int64{0},
		fHeader:    []byte{0, 0, 0, 0},
	}
	f.open()
	return f
}

func (f *FWriter) open() {
	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalln("FWriter.open, 文件创建失败", err)
	}
	f.file = file
	f.reader, err = os.OpenFile(f.path, os.O_RDONLY, 0)
	if err != nil {
		log.Fatalln("FWriter.open, 文件打开失败", err)
	}
	f.bufWriter = bufio.NewWriterSize(f.file, 1024*1024*5)
}

func (f *FWriter) Path() string {
	return f.path
}

func (f *FWriter) GetWriter() *os.File {
	return f.file
}

func (f *FWriter) GetReader() *os.File {
	return f.reader
}

func (f *FWriter) preData(d []byte) []byte {
	//return d
	blen := make([]byte, 8)
	binary.BigEndian.PutUint64(blen, uint64(len(d)))
	var res []byte
	res = append(res, f.fHeader...)
	res = append(res, blen...)
	res = append(res, d...)
	return res
}

func (f *FWriter) Write(d []byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	nn, err := f.file.Write(f.preData(d))
	if err != nil {
		return nn, err
	}
	f.addIndex(len(d))
	return nn, err
}

func (f *FWriter) BatchWrite(arr [][]byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	count := 0
	for _, d := range arr {
		l, err := f.file.Write(f.preData(d))
		if err != nil {
			log.Fatalln("FWriter.BatchWrite.err:", err)
		}
		f.addIndex(len(d))
		count = count + l
	}
	return count, nil
}

func (f *FWriter) WriteToBuf(d []byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	nn, err := f.bufWriter.Write(f.preData(d))
	if err != nil {
		return nn, err
	}
	f.addIndex(len(d))
	f.bufWriter.Flush()
	return nn, err
}

func (f *FWriter) BatchWriteToBuf(arr [][]byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	count := 0
	for _, d := range arr {
		l, err := f.bufWriter.Write(f.preData(d))
		if err != nil {
			log.Fatalln("FWriter.BatchWrite.err:", err)
		}
		f.addIndex(len(d))
		count = count + l
	}
	f.bufWriter.Flush()
	return count, nil
}

func (f *FWriter) Count() int {
	return len(f.offsetList) - 1
}

func (f *FWriter) Flush() {
	f.bufWriter.Flush()
	f.SaveIdxFile()
}
