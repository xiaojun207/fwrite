package fwrite

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
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
	lengthList []uint64
	bufWriter  *bufio.Writer
	mutex      sync.RWMutex
	fHeader    []byte
}

func New(path string) *FWriter {
	os.MkdirAll(path, os.ModePerm)
	f := &FWriter{
		path:       path + "/00000001.f",
		idxPath:    path + "/00000001.i",
		offsetList: []int64{},
		lengthList: []uint64{},
		fHeader:    []byte{0, 0, 0, 0},
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
	f.bufWriter = bufio.NewWriterSize(f.file, 1024*1024*5)
	f.loadIndex()
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
	nn, err := f.bufWriter.Write(f.preData(d))
	if err != nil {
		return nn, err
	}
	f.bufWriter.Flush()
	f.addIndex(len(d))
	return nn, err
}

func (f *FWriter) BatchWriteToBuf(arr [][]byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	count := 0
	for _, d := range arr {
		l, err := f.bufWriter.Write(f.preData(d))
		if err != nil {
			log.Fatalln("BatchWrite.err:", err)
		}
		f.addIndex(len(d))
		count = count + l
	}
	f.bufWriter.Flush()
	return count, nil
}

func (f *FWriter) offset() int64 {
	offset := int64(0)
	count := len(f.offsetList)
	if count > 0 {
		offset = f.offsetList[count-1] + 4 + 8 + int64(f.lengthList[count-1])
	}
	return offset
}

func (f *FWriter) addIndex(l int) {
	offset := f.offset()
	f.offsetList = append(f.offsetList, offset)
	f.lengthList = append(f.lengthList, uint64(l))
}

func (f *FWriter) loadIdxFile() {
	if exists(f.idxPath) {
		reader, err := os.OpenFile(f.idxPath, os.O_RDONLY, 0)
		if err != nil {
			log.Fatalln("文件打开失败", err)
		}
		arr, err := io.ReadAll(reader)
		count := len(arr)
		idx := 0

		f.offsetList = []int64{}
		f.lengthList = []uint64{}
		for idx < count {
			lastOffset := binary.BigEndian.Uint64(arr[idx : idx+8])
			lastLength := binary.BigEndian.Uint64(arr[idx+8 : idx+8+8])

			f.offsetList = append(f.offsetList, int64(lastOffset))
			f.lengthList = append(f.lengthList, lastLength)
			idx = idx + 8 + 8
		}
		log.Println("loadIdxFile, len:", len(f.offsetList))
	}
}

func (f *FWriter) SaveIdxFile() {
	if exists(f.idxPath) {
		os.RemoveAll(f.idxPath)
	}
	file, err := os.OpenFile(f.idxPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalln("文件创建失败", err)
	}

	var arr []byte
	for i := 0; i < len(f.offsetList); i++ {
		offset, length := make([]byte, 8), make([]byte, 8)
		binary.BigEndian.PutUint64(offset, uint64(f.offsetList[i]))
		binary.BigEndian.PutUint64(length, f.lengthList[i])
		arr = append(arr, offset...)
		arr = append(arr, length...)
	}
	file.Write(arr)
}

func (f *FWriter) loadIndex() {
	log.Println("FWriter.loadIndex ...")
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.loadIdxFile()

	idx := len(f.offsetList)
	offset := f.offset()

	f.reader.Seek(0, 0)
	for true {
		var d = make([]byte, 8)
		count, err := f.reader.ReadAt(d, 4+offset)
		if err != nil {
			if err.Error() == "EOF" {
				log.Println("loadIdx, idx:", idx)
			} else {
				log.Println("loadIdx err:", err, ", count:", count, ",idx:", idx)
			}
			break
		}
		if count != 8 {
			log.Println("loadIdx count:", count)
			break
		}
		length := binary.BigEndian.Uint64(d)
		f.addIndex(int(length))
		offset = offset + int64(length) + 8 + 4
		idx++
	}
	f.SaveIdxFile()
}

// index is start at 0
func (f *FWriter) Read(index int) ([]byte, error) {
	if index >= len(f.offsetList) {
		return nil, errors.New("index is out of range")
	}
	if len(f.offsetList) != len(f.lengthList) {
		log.Panicln("FWriter.Read  index[", f.path, "] is err, ", len(f.offsetList), "!=", len(f.lengthList), ", please restart")
	}

	length := f.lengthList[index]
	offset := f.offsetList[index]

	lastOffset, _ := f.reader.Seek(0, 2)

	left := lastOffset - offset - 8 - 4
	if left < int64(length) {
		length = uint64(left)
		log.Println("FWriter.Read, lastOffset:", lastOffset, ",offset:", offset, ",length:", length)
	}

	var b = make([]byte, length)

	c, err := f.reader.ReadAt(b, offset+8+4)
	if uint64(c) != length {
		log.Println("fwrite.Read, count:", c, ", length:", length)
	}
	return b, err
}

func (f *FWriter) Count() int {
	return len(f.offsetList)
}

func (f *FWriter) Flush() {
	f.bufWriter.Flush()
	f.SaveIdxFile()
}
