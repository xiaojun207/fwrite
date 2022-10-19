package fwrite

import (
	"encoding/binary"
	"os"
)

type FMeta struct {
	metaPath string
	bufNum   uint64
	bufSize  uint64
	bufFirst []byte
	bufLast  []byte

	num    uint64 `json:"num"`
	first  []byte `json:"first"`
	last   []byte `json:"last"`
	offset uint64 `json:"offset"`
}

func preMetaData(d []byte) []byte {
	var b []byte
	if len(d) >= FMetaSize {
		b = d[0:FMetaSize]
	} else {
		b = make([]byte, FMetaSize)
		copy(b[FMetaSize-len(d):], d)
	}
	return b
}

func (f *FMeta) fillToMeta(d []byte) {
	f.bufNum++
	f.bufSize += uint64(HeadSize + LengthSide + len(d))

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
	f.num += f.bufNum
	f.bufNum = 0

	if f.bufSize > 0 {
		f.offset += f.bufSize
		f.bufSize = 0
	}

	if f.first == nil {
		f.first = f.bufFirst
	}
	f.last = f.bufLast
	f.bufLast = nil

	d := f.Marshal()
	os.WriteFile(f.metaPath, d, 0666)
}

func (f *FMeta) loadMeta() {
	if exists(f.metaPath) {
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
