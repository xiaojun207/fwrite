package fwrite

import (
	lz4 "github.com/pierrec/lz4/v4"
	"github.com/xiaojun207/fwrite/utils"
	"log"
	"os"
	"strings"
	"sync"
)

type IOWriter interface {
	Write(p []byte) (n int, err error)
	Flush() (err error)
}

type IOReader interface {
	Read(p []byte) (n int, err error)
	ReadAt(p []byte, offset int64) (n int, err error)
}

type LenInt uint16

func toLenArr(ln int) []byte {
	if LengthSide == 2 {
		return utils.Uint16ToByte(uint16(ln))
	} else if LengthSide == 4 {
		return utils.Uint32ToByte(uint32(ln))
	}
	return utils.Uint64ToByte(uint64(ln))
}

func toLenInt(ln []byte) LenInt {
	if LengthSide == 2 {
		return LenInt(utils.ByteToUint16(ln))
	} else if LengthSide == 4 {
		return LenInt(utils.ByteToUint32(ln))
	}
	return LenInt(utils.ByteToUint64(ln))
}

var (
	fHeader = []byte{0}
)

const (
	LengthSide = 4
	HeadSize   = 1
	IdxSize    = 8
	FMetaSize  = 16
)

type FWriter struct {
	FReader
	FMeta
	FIdx
	path   string
	writer IOWriter
	mutex  sync.RWMutex
}

func New(path string) *FWriter {
	fileName := "00000001.f"
	os.MkdirAll(path, os.ModePerm)
	f := &FWriter{
		path: path + "/" + fileName,
		FReader: FReader{
			path: path + "/" + fileName,
		},
		FIdx: FIdx{
			idxPath: path + "/" + strings.TrimRight(fileName, ".f") + ".i",
		},
		FMeta: FMeta{
			metaPath: path + "/meta.m",
		},
	}
	f.open()
	return f
}

func (f *FWriter) open() {
	if !utils.Exists(f.path) {
		return
	}
	if utils.Exists(f.FMeta.metaPath) {
		f.FMeta.loadMeta()
	} else {
		f.recreateMeta()
	}
}

func (f *FWriter) Path() string {
	return f.path
}

func (f *FWriter) GetWriter() IOWriter {
	if f.writer == nil {
		file, err := os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatalln("FWriter.GetWriter.open, 文件创建失败", err)
		}
		fileInfo, _ := os.Stat(f.path)

		w := lz4.NewWriter(file)
		f.writer = w
		err = w.Apply(lz4.ChecksumOption(false), lz4.AppendOption(fileInfo.Size() > 4))

		if err != nil {
			log.Println("FWriter.GetWriter.Apply.err:", err)
		}
	}
	return f.writer
}

func (f *FWriter) preData(d []byte) []byte {
	//d = Lz4(d)
	var res []byte
	res = append(res, fHeader...)
	arrLen := toLenArr(len(d))
	res = append(res, arrLen...)
	res = append(res, d...)
	return res
}

func (f *FWriter) write(d []byte) (int, error) {
	nn, err := f.GetWriter().Write(f.preData(d))
	if err != nil {
		return nn, err
	}

	f.FMeta.fillToMeta(d)
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

func (f *FWriter) flush() {
	if f.writer != nil && f.FMeta.bufNum > 0 {
		f.GetWriter().Flush()
		f.FMeta.flushMeta()
	}
}

func (f *FWriter) Flush() {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.flush()
}

func (f *FWriter) FileSize() int64 {
	return utils.Size(f.path)
}
