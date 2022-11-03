package fwrite

import (
	"github.com/xiaojun207/fwrite/utils"
	"log"
	"os"
	"strconv"
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
	ext           = ".f"
	LengthSide    = 4
	HeadSize      = 1
	IdxSize       = 8
	FMetaDataSize = 16
	FMetaSize     = 8 + FMetaDataSize + FMetaDataSize + IdxSize
	FilePerms     = 0666
)

type FWriter struct {
	FReader
	FMeta
	FIdx
	metaPath string
	path     string
	segLimit uint64 // 定义 segment 记录数
	segments []*FSegment
	mutex    sync.RWMutex
}

func New(path string) *FWriter {
	f := &FWriter{
		path:     path,
		metaPath: path + "/meta.m",
		FReader:  FReader{},
		FIdx: FIdx{
			idxPath: path + "/idx.i",
		},
		segLimit: 50 * 10000,
		FMeta:    FMeta{},
	}
	f.open()
	return f
}

func (f *FWriter) open() {
	if !utils.Exists(f.path) {
		os.MkdirAll(f.path, os.ModePerm)
	}

	f.FReader.segments = &f.segments

	f.loadSegment()

	if utils.Exists(f.metaPath) {
		f.FMeta.loadMetaFromFile(f.metaPath)
	} else {
		f.recreateMeta()
	}
}

func (f *FWriter) recreateMeta() {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	num := uint64(0)
	for i, segment := range f.segments {
		segment.GetReader()
		num += segment.FMeta.num
		if i == 0 {
			f.FMeta.setFirst(segment.FMeta.first)
		}
		if i == len(f.segments)-1 {
			f.FMeta.setLast(segment.FMeta.last)
			f.FMeta.offset = segment.FMeta.offset
		}
	}
	f.FMeta.num = num
	f.FMeta.flushMetaToFile(f.metaPath)
	log.Println("recreateMeta.end,meta:", f.FMeta)
}

func (f *FWriter) loadSegment() error {
	fis, err := os.ReadDir(f.path)
	if err != nil {
		log.Println("loadSegment,err:", err)
		return err
	}
	for _, fi := range fis {
		name := fi.Name()
		if fi.IsDir() || len(name) < 20 || !strings.HasSuffix(name, ext) {
			continue
		}

		index, err := strconv.ParseUint(name[:20], 10, 64)
		if err != nil {
			continue
		}
		f.segments = append(f.segments, newSegment(index, f.path, f.segLimit))
	}
	if len(f.segments) == 0 {
		// Create a new file
		f.segments = append(f.segments, newSegment(f.FMeta.num, f.path, f.segLimit))
	}
	log.Println("loadSegment, count:", len(f.segments))
	return nil
}

func (f *FWriter) Path() string {
	return f.path
}

func (f *FWriter) lastSegment() *FSegment {
	lastSeg := f.segments[len(f.segments)-1]
	if lastSeg.full() {

		lastSeg.flush()
		f.FMeta.flushMetaToFile(f.metaPath)

		seg := newSegment(f.FMeta.num, f.path, f.segLimit)
		f.segments = append(f.segments, seg)
		return seg
	}
	return lastSeg
}

func (f *FWriter) Reset() {
	defer func() {
		f.lastSegment().writer = nil
		if err := recover(); err != nil {
			log.Println("FWriter.Reset, err:", err)
		}
	}()
	f.flush()
	f.lastSegment().writer = nil
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
	segment := f.lastSegment()
	nn, err := segment.write(f.preData(d))
	if err != nil {
		segment.flush()
		segment.reset()
		f.FMeta.flushMetaToFile(f.metaPath)
		//log.Panicln("FWriter.write, err:", err)
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
	lastSeg := f.lastSegment()
	if lastSeg.writer != nil && f.FMeta.bufNum > 0 {
		lastSeg.flush()
		f.FMeta.flushMetaToFile(f.metaPath)
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
