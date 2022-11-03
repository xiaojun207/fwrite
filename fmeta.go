package fwrite

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"sync"
)

var emptyMetaData = make([]byte, FMetaDataSize)

type FMeta struct {
	bufNum     uint64
	bufSize    uint64
	lastLength uint64
	bufFirst   []byte
	bufLast    []byte
	metaMutex  sync.RWMutex

	num    uint64 `json:"num"`    // 8 byte
	first  []byte `json:"first"`  // 16 byte
	last   []byte `json:"last"`   // 16 byte
	offset uint64 `json:"offset"` // 8 byte
}

func preMetaData(d []byte) []byte {
	if len(d) >= FMetaDataSize {
		return d[0:FMetaDataSize]
	} else {
		var b = make([]byte, FMetaDataSize)
		copy(b[0:len(d)], d)
		return b
	}
}

func (f *FMeta) firstEmpty() bool {
	return len(f.first) == 0 || bytes.Equal(f.first, emptyMetaData)
}

func (f *FMeta) fillToMeta(d []byte) {
	f.bufNum++
	f.lastLength = uint64(HeadSize + LengthSide + len(d))
	f.bufSize += f.lastLength

	f.bufLast = preMetaData(d)

	if len(f.bufFirst) == 0 {
		f.bufFirst = f.bufLast
	}
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
	f.first = b[i : i+FMetaDataSize]
	i += FMetaDataSize
	f.last = b[i : i+FMetaDataSize]
	i += FMetaDataSize
	f.offset = binary.BigEndian.Uint64(b[i : i+IdxSize])
}

func (f *FMeta) readMeta(r io.Reader) (n int, err error) {
	buf := make([]byte, FMetaSize)
	n, err = r.Read(buf)
	f.Unmarshal(buf)
	return n, err
}

func (f *FMeta) flushMeta(w io.Writer) (int, error) {
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

		if f.firstEmpty() {
			f.first = f.bufFirst
		}
		f.last = f.bufLast
		f.bufLast = nil
	}
	if len(f.last) == 0 {
		f.first = make([]byte, FMetaDataSize)
	}
	if len(f.last) == 0 || bytes.Equal(f.last, emptyMetaData) {
		f.last = f.first
	}
	d := f.Marshal()
	return w.Write(d)
}

func (f *FMeta) flushMetaToFile(path string) {
	fmw, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err == nil {
		f.flushMeta(fmw)
	}
}

func (f *FMeta) loadMetaFromFile(path string) {
	fmr, _ := os.Open(path)
	f.readMeta(fmr)
}

// LastData last write data, only show first 16 byte
func (f *FMeta) LastData() []byte {
	return f.last
}

// FirstData first write data, only show first 16 byte
func (f *FMeta) FirstData() []byte {
	return f.first
}

func (f *FMeta) String() string {
	m := map[string]interface{}{
		"num":    f.num,
		"first":  f.first,
		"last":   f.last,
		"offset": f.offset,
	}
	d, _ := json.Marshal(m)
	return string(d)
}
