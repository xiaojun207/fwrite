package fwrite

import (
	lz4 "github.com/pierrec/lz4/v4"
	"log"
	"os"
	"sync"
)

type IOWriter interface {
	Write(p []byte) (n int, err error)
	Flush() (err error)
}

type IOReader interface {
	Read(p []byte) (n int, err error)
}

type LenInt uint16

const (
	LengthSide = 2
	HeadSize   = 1
)

type FWriter struct {
	path       string
	idxPath    string
	file       *os.File
	offsetList []int64
	writer     IOWriter
	reader     IOReader
	mutex      sync.RWMutex
	readLock   sync.RWMutex
	readOffset int64
	idxHasLoad bool
	idxOffset  int64
	fHeader    []byte
	count      int
}

func New(path string) *FWriter {
	os.MkdirAll(path, os.ModePerm)
	f := &FWriter{
		path:       path + "/00000001.f",
		idxPath:    path + "/00000001.i",
		offsetList: []int64{0},
		fHeader:    []byte{0},
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
}

func (f *FWriter) Path() string {
	return f.path
}

func (f *FWriter) GetWriter() IOWriter {
	if f.writer == nil {
		w := lz4.NewWriter(f.file)
		f.writer = w
		fileInfo, _ := os.Stat(f.path)
		err := w.Apply(lz4.ChecksumOption(false), lz4.AppendOption(fileInfo.Size() > 4))
		if err != nil {
			log.Println("GetWriter.Apply.err:", err)
		}
	}
	return f.writer
}

func (f *FWriter) GetReader() (reader IOReader) {
	file, err := os.OpenFile(f.path, os.O_RDONLY, 0)
	if err != nil {
		log.Fatalln("FWriter.GetReader, 文件打开失败", err)
	}
	reader = lz4.NewReader(file)
	return
}

func (f *FWriter) toLenArr(ln int) []byte {
	if LengthSide == 2 {
		return Uint16ToByte(uint16(ln))
	} else if LengthSide == 4 {
		return Uint32ToByte(uint32(ln))
	}
	return Uint64ToByte(uint64(ln))
}

func (f *FWriter) toLenInt(ln []byte) LenInt {
	if LengthSide == 2 {
		return LenInt(ByteToUint16(ln))
	} else if LengthSide == 4 {
		return LenInt(ByteToUint32(ln))
	}
	return LenInt(ByteToUint64(ln))
}

func (f *FWriter) preData(d []byte) []byte {
	//d = Lz4(d)
	var res []byte
	res = append(res, f.fHeader...)
	arrLen := f.toLenArr(len(d))
	res = append(res, arrLen...)
	res = append(res, d...)

	return res
}

func (f *FWriter) Write(d []byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	nn, err := f.GetWriter().Write(f.preData(d))
	if err != nil {
		return nn, err
	}
	f.addOffset(nn)
	return nn, err
}

func (f *FWriter) BatchWrite(arr [][]byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	count := 0
	for _, d := range arr {
		l, err := f.GetWriter().Write(f.preData(d))
		if err != nil {
			log.Fatalln("FWriter.BatchWrite.err:", err)
		}
		f.addOffset(l)
		count = count + l
	}
	return count, nil
}

func (f *FWriter) Count() int {
	return f.count
}

func (f *FWriter) Flush() {
	if f.writer != nil && f.count != len(f.offsetList)-1 {
		f.GetWriter().Flush()
		f.count = len(f.offsetList) - 1
	}
}
