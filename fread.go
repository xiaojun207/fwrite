package fwrite

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/xiaojun207/fwrite/flz4"
	"github.com/xiaojun207/fwrite/utils"
	"log"
)

type IOReader interface {
	Read(p []byte) (n int, err error)
	ReadAt(p []byte, offset int64) (n int, err error)
}

type FRowFilter func(idx uint64, offset int64, length LenInt, d []byte) bool
type FSegFilter func(index, num uint64, first, last []byte, offset uint64) bool

type FReader struct {
	segments *[]*FSegment
	readers  []IOReader
}

func (f *FReader) LoadReader() {
	if len(f.readers) == len(*f.segments) {
		return
	}
	f.readers = f.loadReader()
}

func (f *FReader) loadReader() []IOReader {
	var readers []IOReader
	for _, segment := range *f.segments {
		sr := segment.GetReader()
		readers = append(readers, sr)
	}
	return readers
}

func (f *FReader) Search(query []byte) (count int) {
	for _, segment := range *f.segments {
		fileMMap := segment.MMap()

		idxes := utils.Indexes(fileMMap, query)
		//log.Println("i:", i, "FReader.query:", query, ",count:", len(idxes))
		for _, _ = range idxes {
			//log.Println("idx:", idx, ",fileMMap[idx-15:idx+14]:", string(fileMMap[idx-104:idx+140]))
			//log.Println("idx:", idx, ",fileMMap[idx-260:idx+140]:", fileMMap[idx-107:idx+140])
			count++
		}
	}
	return
}

// foreachAll reset reader read all
func (f *FReader) foreachAll(segFilter FSegFilter, filter FRowFilter) (idx uint64, err error) {
	readers := f.loadReader()

	for i, reader := range readers {
		seg := (*f.segments)[i]

		if segFilter != nil && !segFilter(seg.index, seg.num, seg.first, seg.last, seg.size) {
			continue
		}

		n, e := foreachSingle(reader, idx, 0, filter)
		//n, e := f.foreachOne(reader, idx, 0, filter)
		if e != nil {
			if e.Error() == "EOF" {
				//
			} else {
				log.Println("foreachFull,err:", e, ", i:", i, ",index:", seg.index, ",n:", n)
				//return idx, e
			}
		}
		idx += n
	}
	return idx, err
}

// foreachSingle reset reader read all
func foreachSingle(reader IOReader, startIdx uint64, offset int64, filter FRowFilter) (idx uint64, err error) {
	if rs, ok := reader.(flz4.Reset); ok {
		rs.Reset()
	}

	length := LenInt(0)
	idx = startIdx
	over := false
	var recv = flz4.Receiver{}
	// 每个块数据完整
	var leftBuf bytes.Buffer
	var i = 0
	recv.OnRead = func(d []byte) {
		if over {
			return
		}
		if leftBuf.Len() > 0 {
			d = append(leftBuf.Bytes(), d...)
			leftBuf.Reset()
		}
		i++
		cur := 0
		dLen := len(d)
		for dLen > cur {
			off := 0
			if !bytes.HasPrefix(d[cur:], fHeaderFlag) {
				log.Println("i:", i, ",foreachSingle,head1,startIdx:", startIdx, ",header,.dLen:", dLen, ",cur:", cur, ",length:", length, ",d[cur:]", d[cur:cur+20])

				if EndSize > 0 {
					// 发现错误数据，跳过
					skipIdx := bytes.Index(d[cur:], fEndFlag)
					if skipIdx > -1 {
						cur += skipIdx + len(fEndFlag)
						continue
					}
				}
				break
			}
			off += HeadSize
			if dLen < cur+off+LengthSide {
				log.Println("i:", i, ",foreachSingle,head2,startIdx:", startIdx, ",header,.dLen:", dLen, ",cur:", cur, ",length:", length, ",d[cur:]")
				break
			}
			length = toLenInt(d[cur+off : cur+off+LengthSide])
			off += LengthSide

			if dLen < cur+off+int(length) {
				break
			}
			if !filter(idx, offset, length, d[cur+off:cur+off+int(length)]) {
				over = true
				return
			}
			off += int(length)
			off += EndSize
			cur += off
			offset += int64(off)
			idx++
		}
		if dLen != cur {
			leftBuf.Write(d[cur:])
		}
	}
	if rs, ok := reader.(flz4.Reset); ok {
		rs.Reset()
	}
	_, err = flz4.Copy(recv, reader)
	return idx, err
}

// foreachOne reset reader read all
func (f *FReader) foreachOne(reader IOReader, startIdx uint64, offset int64, filter FRowFilter) (idx uint64, err error) {
	length := LenInt(0)
	idx = startIdx
	if rs, ok := reader.(flz4.Reset); ok {
		rs.Reset()
	}
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
		offset += EndSize
		idx++
	}
	return idx, nil
}

// Foreach reset reader read all
func (f *FReader) Foreach(segFilter FSegFilter, filter FRowFilter) (idx uint64, err error) {
	return f.foreachAll(segFilter, filter)
}

func (f *FReader) readAt(reader IOReader, p []byte, offset int64) (n int, err error) {
	return reader.ReadAt(p, offset)
}

// read , depend on idx
func (f *FWriter) read(index int) ([]byte, error) {
	f.LoadReader()
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

	useIdx := false
	//useIdx = false

	if useIdx {
		offset, length := f.FIdx.getOffset(index)
		var b = make([]byte, length)
		_, err := reader.ReadAt(b, int64(offset))
		return b, err
	} else {
		var b []byte
		_, err := foreachSingle(reader, segment.index, 0, func(idx uint64, offset int64, length LenInt, d []byte) bool {
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
