package fwrite

import (
	"encoding/binary"
	"github.com/pierrec/lz4/v4"
	"log"
	"os"
)

const (
	IdxSize = 8
)

func (f *FWriter) offset() int64 {
	offset := int64(0)
	count := len(f.offsetList)
	if count > 0 {
		offset = f.offsetList[count-1]
	}
	return offset
}

func (f *FWriter) addOffset(l int) {
	offset := f.offset()
	f.offsetList = append(f.offsetList, offset+int64(l))
}

func (f *FWriter) getOffset(index int) int64 {
	return f.offsetList[index]
}

func (f *FWriter) loadIdxFile() {
	if exists(f.idxPath) {
		file, err := os.Open(f.idxPath)
		if err != nil {
			log.Fatalln("FWriter.loadIdxFile.文件打开失败", err)
		}
		reader := lz4.NewReader(file)
		f.offsetList = []int64{}
		for true {
			var p = make([]byte, IdxSize)
			_, err = reader.Read(p)
			if err != nil {
				if err.Error() != "EOF" {
					log.Println("loadIdxFile.Read.err:", err)
				}
				break
			}
			lastOffset := ByteToUint64(p)
			f.offsetList = append(f.offsetList, int64(lastOffset))
			f.idxOffset = int64(len(f.offsetList) - 1)
		}
	}
}

func (f *FWriter) SaveIdxFile() {
	file, err := os.OpenFile(f.idxPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalln("FWriter.SaveIdxFile.文件创建失败:", err)
	}
	w := lz4.NewWriter(file)
	fileInfo, _ := os.Stat(f.idxPath)
	w.Apply(lz4.ChecksumOption(false), lz4.AppendOption(fileInfo.Size() > 4))

	idxOffset := f.idxOffset
	has := fileInfo.Size() > 4

	var arr []byte
	for i := 0; i < len(f.offsetList); i++ {
		if has && i <= int(f.idxOffset) {
			continue
		}

		arr = binary.BigEndian.AppendUint64(arr, uint64(f.offsetList[i]))
		idxOffset = int64(i)
	}
	if len(arr) > 0 {
		log.Println("FWriter.SaveIdxFile, AddNum:", len(arr)/IdxSize)
		w.Write(arr)
		w.Flush()
		f.idxOffset = idxOffset
	}
}

func (f *FWriter) loadIdxFromData() int {
	idx := 0
	offset := f.offset()
	for true {
		var d = make([]byte, LengthSide)
		count, err := f.readAt(d, HeadSize+offset)
		if err != nil {
			if err.Error() != "EOF" {
				log.Println("FWriter.loadIdxFromData err:", err, ", count:", count, ",idx:", idx, ",offset:", offset)
			}
			break
		}
		length := f.toLenInt(d)
		f.addOffset(int(length + LengthSide + HeadSize))
		offset = offset + int64(length) + LengthSide + HeadSize
		idx++
	}
	return idx
}

func (f *FWriter) LoadIndex() {
	if f.idxHasLoad {
		return
	}
	log.Println("FWriter[" + f.path + "].LoadIndex ...")
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.idxHasLoad = true

	f.loadIdxFile()
	num := f.loadIdxFromData()

	f.count = len(f.offsetList) - 1

	log.Println("FWriter["+f.path+"].LoadIndex: ", f.Count())
	if num > 0 {
		log.Println("FWriter["+f.path+"].LoadIndex: num:", num)
		f.SaveIdxFile()
	}
}
