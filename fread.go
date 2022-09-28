package fwrite

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
)

func (f *FWriter) offset() int64 {
	offset := int64(0)
	count := len(f.offsetList)
	if count > 0 {
		offset = f.offsetList[count-1]
	}
	return offset
}

func (f *FWriter) addIndex(l int) {
	offset := f.offset()
	f.offsetList = append(f.offsetList, offset+4+8+int64(l))
}

func (f *FWriter) loadIdxFile() {
	if exists(f.idxPath) {
		reader, err := os.OpenFile(f.idxPath, os.O_RDONLY, 0)
		if err != nil {
			log.Fatalln("FWriter.loadIdxFile.文件打开失败", err)
		}
		arr, err := io.ReadAll(reader)
		count := len(arr)
		idx := 0

		f.offsetList = []int64{}
		for idx < count {
			lastOffset := binary.BigEndian.Uint64(arr[idx : idx+8])
			f.offsetList = append(f.offsetList, int64(lastOffset))
			idx = idx + 8
		}
		log.Println("FWriter.loadIdxFile, len:", len(f.offsetList)-1)
	}
}

func (f *FWriter) SaveIdxFile() {
	if exists(f.idxPath) {
		os.RemoveAll(f.idxPath)
	}
	file, err := os.OpenFile(f.idxPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalln("FWriter.SaveIdxFile.文件创建失败:", err)
	}

	var arr []byte
	for i := 0; i < len(f.offsetList); i++ {
		offset := make([]byte, 8)
		binary.BigEndian.PutUint64(offset, uint64(f.offsetList[i]))
		arr = append(arr, offset...)
	}
	file.Write(arr)
}

func (f *FWriter) LoadIndex() {
	log.Println("FWriter.LoadIndex ...")
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.loadIdxFile()

	idx := len(f.offsetList) - 1
	offset := f.offset()

	f.reader.Seek(0, 0)
	for true {
		var d = make([]byte, 8)
		count, err := f.reader.ReadAt(d, 4+offset)
		if err != nil {
			if err.Error() == "EOF" {
				log.Println("FWriter.LoadIdx, idx:", idx)
			} else {
				log.Println("FWriter.LoadIdx err:", err, ", count:", count, ",idx:", idx)
			}
			break
		}
		if count != 8 {
			log.Println("FWriter.LoadIdx count:", count)
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
	if index >= len(f.offsetList)-1 {
		return nil, errors.New("FWriter read, index is out of range")
	}

	offset := f.offsetList[index]
	offsetNext := f.offsetList[index+1]
	length := offsetNext - offset - 8 - 4

	lastOffset, _ := f.reader.Seek(0, 2)

	left := lastOffset - offset - 8 - 4
	if left < length {
		length = left
		log.Println("FWriter.Read, lastOffset:", lastOffset, ",offset:", offset, ",length:", length)
	}

	var b = make([]byte, length)
	c, err := f.reader.ReadAt(b, offset+8+4)
	if int64(c) != length {
		log.Println("FWriter.Read, count:", c, ", length:", length)
	}
	return b, err
}
