package fwrite

import (
	"bufio"
	"encoding/binary"
	"errors"
	"log"
	"os"
	"sync"
)

type FWriter struct {
	path       string
	reader     *os.File
	file       *os.File
	indexList  []int64
	lengthList []uint64
	bufWriter  *bufio.Writer
	mutex      sync.RWMutex
}

func New(path string) *FWriter {
	os.MkdirAll(path, os.ModePerm)
	f := &FWriter{
		path:       path + "/00000001.f",
		indexList:  []int64{},
		lengthList: []uint64{},
	}
	f.open()
	return f
}

func (f *FWriter) open() {
	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalln("文件创建失败", err)
	}
	f.file = file
	f.reader, err = os.OpenFile(f.path, os.O_RDONLY, 0)
	if err != nil {
		log.Fatalln("文件打开失败", err)
	}
	f.bufWriter = bufio.NewWriter(f.file)
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

func preData(d []byte) []byte {
	//return d
	blen := make([]byte, 8)
	binary.BigEndian.PutUint64(blen, uint64(len(d)))
	return append(blen, d...)
}

func (f *FWriter) Write(d []byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	nn, err := f.file.Write(preData(d))
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
		l, err := f.file.Write(preData(d))
		if err != nil {
			log.Fatalln("BatchWrite.err:", err)
		}
		f.addIndex(len(d))
		count = count + l
	}
	return count, nil
}

func (f *FWriter) WriteToBuf(d []byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	nn, err := f.bufWriter.Write(preData(d))
	if err != nil {
		return nn, err
	}
	f.addIndex(len(d))
	return nn, err
}

func (f *FWriter) BatchWriteToBuf(arr [][]byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	count := 0
	for _, d := range arr {
		l, err := f.bufWriter.Write(preData(d))
		if err != nil {
			log.Fatalln("BatchWrite.err:", err)
		}
		f.addIndex(len(d))
		count = count + l
	}
	return count, nil
}

func (f *FWriter) addIndex(l int) {
	offset := int64(0)
	count := len(f.indexList)
	if count > 0 {
		offset = f.indexList[count-1] + 8 + int64(f.lengthList[count-1])
	}
	f.indexList = append(f.indexList, offset)
	f.lengthList = append(f.lengthList, uint64(l))
}

func (s *FWriter) loadIndex() {
	idx := 0
	offset := int64(0)
	s.reader.Seek(0, 0)
	for true {
		var d = make([]byte, 8)
		count, err := s.reader.ReadAt(d, offset)
		if err != nil {
			log.Println("loadIdx err:", err, ", count:", count, ",idx:", idx)
			break
		}
		if count != 8 {
			log.Println("loadIdx count:", count)
			break
		}
		length := binary.BigEndian.Uint64(d)

		s.addIndex(int(length))

		offset = offset + int64(length) + 8
		idx++
	}
}

// index is start at 0
func (f *FWriter) Read(index int) ([]byte, error) {
	if index >= len(f.indexList) {
		return nil, errors.New("index is out of range")
	}
	if len(f.indexList) != len(f.lengthList) {
		log.Panicln("FWriter index[", f.path, "] is err, ", len(f.indexList), "!=", len(f.lengthList), ", please restart")
	}

	length := f.lengthList[index]
	offset := f.indexList[index]
	var b = make([]byte, length)
	c, err := f.reader.ReadAt(b, offset+8)
	if uint64(c) != length {
		log.Println("fwrite.Read, count")
	}
	return b, err
}

func (f *FWriter) Count() int {
	return len(f.indexList)
}

func (f *FWriter) Flush() {
	f.bufWriter.Flush()
}
