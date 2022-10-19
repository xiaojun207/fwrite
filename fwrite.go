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
	IdxSize    = 8
	FMetaSize  = 16
)

type FWriter struct {
	FReader
	FMeta
	FIdx
	path    string
	writer  IOWriter
	mutex   sync.RWMutex
	fHeader []byte
}

func New(path string) *FWriter {
	os.MkdirAll(path, os.ModePerm)
	f := &FWriter{
		path: path + "/00000001.f",
		FReader: FReader{
			path: path + "/00000001.f",
		},
		FIdx: FIdx{
			idxPath:    path + "/00000001.i",
			offsetList: []int64{0},
		},
		FMeta: FMeta{
			metaPath: path + "/meta.m",
		},
		fHeader: []byte{0},
	}
	f.open()
	return f
}

func (f *FWriter) open() {
	f.CreateIdxMeta()
}

func (f *FWriter) Path() string {
	return f.path
}

func (f *FWriter) GetWriter() IOWriter {
	if f.writer == nil {
		file, err := os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatalln("FWriter.open, 文件创建失败", err)
		}
		fileInfo, _ := os.Stat(f.path)

		w := lz4.NewWriter(file)
		f.writer = w
		err = w.Apply(lz4.ChecksumOption(false), lz4.AppendOption(fileInfo.Size() > 4))

		if err != nil {
			log.Println("GetWriter.Apply.err:", err)
		}
	}
	return f.writer
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

func (f *FWriter) write(d []byte) (int, error) {
	nn, err := f.GetWriter().Write(f.preData(d))
	if err != nil {
		return nn, err
	}

	f.fillToMeta(d)
	f.bufNum++
	f.bufSize += uint64(nn)

	f.addOffset(nn)
	return nn, err
}

func (f *FWriter) Write(d []byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.write(d)
}

func (f *FWriter) BatchWrite(arr [][]byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	count := 0
	for _, d := range arr {
		l, err := f.write(d)
		if err != nil {
			log.Fatalln("FWriter.BatchWrite.err:", err)
		}
		count = count + l
	}
	return count, nil
}

func (f *FWriter) Count() uint64 {
	return f.FMeta.num
}

func (f *FWriter) Flush() {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.writer != nil && f.FMeta.bufNum > 0 {
		f.GetWriter().Flush()

		f.FMeta.num += f.FMeta.bufNum
		f.FMeta.bufNum = 0

		if f.FMeta.bufSize > 0 {
			f.FMeta.offset += f.FMeta.bufSize
			f.FMeta.bufSize = 0
		}

		f.flushMeta()
		log.Println("FWriter.Flush,:", f.FMeta)
	}
}
