package fwrite

import (
	"bufio"
	"github.com/edsrzf/mmap-go"
	"log"
	"os"
	"sync"
)

type FIdx struct {
	idxPath    string
	idxWriter  IOWriter
	lastOffset uint64
	idxNum     uint64
	offsetMMap mmap.MMap
	offsetList []uint64
	idxMutex   sync.RWMutex
}

func (f *FIdx) addOffset(l uint64) {
	w := f.getIdxWriter()
	f.lastOffset += l
	w.Write(Uint64ToByte(f.lastOffset))
}

func (f *FIdx) getOffset(index int) (offset uint64, length uint64) {
	nextOffset := ByteToUint64(f.offsetMMap[index*IdxSize : index*IdxSize+IdxSize])
	//nextOffset := f.offsetList[index]
	if index > 0 {
		offset = ByteToUint64(f.offsetMMap[(index-1)*IdxSize : (index-1)*IdxSize+IdxSize])
		//offset = f.offsetList[index-1]
	} else {
		offset = 0
	}
	length = nextOffset - offset - (HeadSize + LengthSide)
	return
}

func (f *FIdx) loadIdxMMap() {
	file, err := os.Open(f.idxPath)
	if err != nil {
		log.Println("FWriter.loadIdxMMap.文件打开失败", err)
		return
	}
	f.offsetMMap, err = mmap.Map(file, mmap.RDONLY, 0)
	count := len(f.offsetMMap)
	if count > 0 {
		f.lastOffset = ByteToUint64(f.offsetMMap[count-IdxSize : count])
	} else {
		f.lastOffset = 0
	}
	f.idxNum = uint64(len(f.offsetMMap) / IdxSize)
	//log.Println("loadIdxMMap.mmap to list")
	//var offsetList []uint64
	//for i := uint64(0); i < f.idxNum; i++ {
	//	offset := ByteToUint64(f.offsetMMap[i*IdxSize : i*IdxSize+IdxSize])
	//	offsetList = append(offsetList, offset)
	//}
	//f.offsetList = offsetList
	//log.Println("loadIdxMMap.mmap to list end")
}

func (f *FIdx) getIdxNum() uint64 {
	return f.idxNum
}

func (f *FIdx) getIdxWriter() IOWriter {
	if f.idxWriter == nil {
		file, err := os.OpenFile(f.idxPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatalln("FWriter.SaveIdxFile.文件创建失败:", err)
		}
		f.idxWriter = bufio.NewWriterSize(file, 1024*1024*15)
		f.idxWriter.Flush()
	}
	return f.idxWriter
}

func (f *FIdx) flushIdx() {
	f.getIdxWriter().Flush()
}

func (f *FWriter) LoadIdx() uint64 {
	return f.loadIdx()
}

func (f *FWriter) loadIdx() uint64 {
	f.FIdx.idxMutex.Lock()
	defer f.FIdx.idxMutex.Unlock()

	if f.offsetMMap == nil {
		f.FIdx.loadIdxMMap()

		if f.FMeta.num != f.FIdx.idxNum {
			f.createIdx()
			f.FIdx.loadIdxMMap()
		}
	}
	return f.FIdx.idxNum
}

func (f *FWriter) createIdx() {
	log.Println("createIdx", ", FMeta.num:", f.FMeta.num, ", FIdx.idxNum:", f.FIdx.idxNum)
	startOffset := f.lastOffset
	f.FReader.foreach(int64(startOffset), func(idx uint64, offset int64, length LenInt, d []byte) bool {
		f.FIdx.addOffset(uint64(length + LengthSide + HeadSize))
		return true
	})
	f.FIdx.flushIdx()
	log.Println("createIdx.end", ", FMeta.num:", f.FMeta.num, ", FIdx.idxNum:", f.FIdx.idxNum)
}
