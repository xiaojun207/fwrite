package fwrite

import (
	"encoding/binary"
	"github.com/xiaojun207/fwrite/utils"
	"log"
	"os"
	"sync"
)

type FMeta struct {
	metaPath   string
	bufNum     uint64
	bufSize    uint64
	lastLength uint64
	bufFirst   []byte
	bufLast    []byte
	metaMutex  sync.RWMutex

	num    uint64 `json:"num"`
	first  []byte `json:"first"`
	last   []byte `json:"last"`
	offset uint64 `json:"offset"`
}

func preMetaData(d []byte) []byte {
	if len(d) >= FMetaSize {
		return d[0:FMetaSize]
	} else {
		var b = make([]byte, FMetaSize)
		copy(b[0:len(d)], d)
		return b
	}
}

func (f *FMeta) fillToMeta(d []byte) {
	f.bufNum++
	f.lastLength = uint64(HeadSize + LengthSide + len(d))
	f.bufSize += f.lastLength

	if f.first == nil && f.bufFirst == nil {
		f.bufFirst = preMetaData(d)
	}
	f.bufLast = preMetaData(d)
}

func (f *FMeta) setFirst(d []byte) {
	f.first = preMetaData(d)
}

func (f *FMeta) setLast(d []byte) {
	f.last = preMetaData(d)
}

func (f *FMeta) Marshal() []byte {
	var res []byte
	res = binary.BigEndian.AppendUint64(res, f.num)
	res = append(res, f.first...)
	res = append(res, f.last...)
	res = binary.BigEndian.AppendUint64(res, f.offset)
	return res
}

func (f *FMeta) Unmarshal(b []byte) {
	i := 0
	f.num = binary.BigEndian.Uint64(b[i : i+8])
	i += 8
	f.first = b[i : i+FMetaSize]
	i += FMetaSize
	f.last = b[i : i+FMetaSize]
	i += FMetaSize
	f.offset = binary.BigEndian.Uint64(b[i : i+IdxSize])
}

func (f *FMeta) flushMeta() {
	f.metaMutex.Lock()
	defer f.metaMutex.Unlock()

	if f.bufNum > 0 {
		f.num += f.bufNum
		f.bufNum = 0

		if f.bufSize > 0 {
			f.offset += f.bufSize - f.lastLength
			f.bufSize = 0
			f.lastLength = 0
		}

		if f.first == nil {
			f.first = f.bufFirst
		}
		f.last = f.bufLast
		f.bufLast = nil
	}

	d := f.Marshal()
	os.WriteFile(f.metaPath, d, 0666)
}

func (f *FMeta) loadMeta() {
	if utils.Exists(f.metaPath) {
		b, _ := os.ReadFile(f.metaPath)
		f.Unmarshal(b)
	}
}

// LastData last write data, only show first 16 byte
func (f *FMeta) LastData() []byte {
	return f.last
}

// FirstData first write data, only show first 16 byte
func (f *FMeta) FirstData() []byte {
	return f.first
}

func (f *FWriter) recreateMeta() {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	lastOffset := int64(0)

	firstLength := LenInt(0)
	lastLength := LenInt(0)
	num := f.foreach(0, func(idx uint64, offset int64, length LenInt, d []byte) bool {
		if idx == 0 {
			firstLength = length
		}
		lastLength = length
		lastOffset = offset
		return true
	})

	if num > 0 {
		var first = make([]byte, firstLength)
		f.readAt(first, HeadSize+LengthSide)
		f.FMeta.setFirst(first)

		var last = make([]byte, lastLength)
		f.readAt(last, lastOffset+HeadSize+LengthSide)
		f.FMeta.setLast(last)
	}

	f.FMeta.num = num
	f.FMeta.offset = uint64(lastOffset)
	f.flushMeta()

	log.Println("recreateMeta.end,meta:", f.FMeta)
}
