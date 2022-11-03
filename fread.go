package fwrite

import (
	"errors"
	"fmt"
)

type FRowFilter func(idx uint64, offset int64, length LenInt, d []byte) bool
type FSegFilter func(index, num uint64, first, last []byte, offset uint64) bool

type FReader struct {
	segments *[]*FSegment
	readers  []IOReader
}

func (f *FReader) GetReader() (reader IOReader) {
	f.readers = []IOReader{}
	for _, segment := range *f.segments {
		sr := segment.GetReader()
		f.readers = append(f.readers, sr)
		reader = sr
	}
	return nil
}

// foreachOne reset reader read all
func (f *FReader) foreachAll(segFilter FSegFilter, filter FRowFilter) (idx uint64, err error) {
	if len(f.readers) == 0 {
		f.GetReader()
	}

	for i, reader := range f.readers {
		seg := (*f.segments)[i]

		if segFilter != nil && !segFilter(seg.index, seg.num, seg.first, seg.last, seg.offset) {
			continue
		}

		n, e := f.foreachOne(reader, idx, 0, filter)
		if e != nil {
			if e.Error() == "EOF" {
				//
			} else {
				return idx, e
			}
		}
		idx += n
	}
	return idx, err
}

// foreachOne reset reader read all
func (f *FReader) foreachOne(reader IOReader, startIdx uint64, offset int64, filter FRowFilter) (idx uint64, err error) {
	length := LenInt(0)
	idx = startIdx
	for true {
		offset += HeadSize
		var ln = make([]byte, LengthSide)
		_, err = reader.ReadAt(ln, offset)
		if err != nil {
			if err.Error() != "EOF" {
				return idx, err
			}
			break
		}
		length = toLenInt(ln)

		offset += LengthSide

		var b = make([]byte, length)
		_, err = reader.ReadAt(b, offset)

		if err != nil {
			if err.Error() != "EOF" {
				return idx, err
			}
			break
		}
		if !filter(idx, offset, length, b) {
			return
		}

		offset += int64(length)
		idx++
	}
	return idx, nil
}

func (f *FReader) readAt(reader IOReader, p []byte, offset int64) (n int, err error) {
	return reader.ReadAt(p, offset)
}

// Foreach reset reader read all
func (f *FReader) Foreach(segFilter FSegFilter, filter FRowFilter) (idx uint64, err error) {
	return f.foreachAll(segFilter, filter)
}

// read , depend on idx
func (f *FWriter) read(index int) ([]byte, error) {
	if len(f.readers) == 0 {
		f.GetReader()
	}
	// 0,10,20     5
	segIdx := 0
	for i, segment := range *f.FReader.segments {
		if segment.index <= uint64(index) {
			segIdx = i
		} else {
			break
		}
	}
	segment := (*f.FReader.segments)[segIdx]
	reader := f.readers[segIdx]

	useIdx := true
	//useIdx = false

	if useIdx {
		offset, length := f.FIdx.getOffset(index)
		var b = make([]byte, length)
		_, err := reader.ReadAt(b, int64(offset))
		return b, err
	} else {
		var b []byte
		_, err := f.FReader.foreachOne(reader, segment.index, 0, func(idx uint64, offset int64, length LenInt, d []byte) bool {
			if idx < uint64(index) {
				return true
			} else if idx == uint64(index) {
				b = d
				return false
			} else {
				return false
			}
		})
		return b, err
	}
}

// index is start at 0,  depend on idx
func (f *FWriter) Read(index uint64) ([]byte, error) {
	f.loadIdx()
	if index >= f.FIdx.getIdxNum() {
		return nil, errors.New(fmt.Sprint("FWriter read, index is out of range, index:", index, ",idxNum:", f.FIdx.getIdxNum()))
	}
	return f.read(int(index))
}

// Search ,  depend on idx
func (f *FWriter) Search(filter func(d []byte) bool) (res [][]byte, err error) {
	f.foreachAll(nil, func(idx uint64, offset int64, length LenInt, d []byte) bool {
		if filter(d) {
			res = append(res, d)
		}
		return true
	})
	return res, nil
}
