package fwrite

import (
	"bufio"
	"encoding/binary"
	"github.com/edsrzf/mmap-go"
	"github.com/pierrec/lz4/v4"
	"io"
	"log"
	"os"
)

type FIdx struct {
	idxPath    string
	offsetList []int64
	offsetMMap mmap.MMap
	idxHasLoad bool
	idxOffset  int64
}

var UseLz4 = false

func (f *FIdx) offset() int64 {
	offset := int64(0)
	count := len(f.offsetList)
	if count > 0 {
		offset = f.offsetList[count-1]
	}
	return offset
}

func (f *FIdx) addOffset(l int) {
	offset := f.offset()
	f.offsetList = append(f.offsetList, offset+int64(l))
}

func (f *FIdx) getLength(index int) (offset int64, length int64) {
	offset = f.offsetList[index]
	offsetNext := f.offsetList[index+1]
	return offset, offsetNext - offset - LengthSide - HeadSize
}

func (f *FIdx) getOffset(index int) int64 {
	if len(f.offsetList) == 0 {
		return 0
	}
	return f.offsetList[index-1]
	//i := index * IdxSize
	//return ByteToUint64(f.offsetMMap[i : i+IdxSize])
}

func (f *FIdx) loadIdxMMap() {
	if exists(f.idxPath) {
		file, err := os.Open(f.idxPath)
		if err != nil {
			log.Fatalln("FWriter.loadIdxFile.文件打开失败", err)
		}
		f.offsetMMap, err = mmap.Map(file, mmap.RDONLY, 0)
		//f.FMeta.num = uint64(len(f.offsetMMap) / 8)
	}
}

func (f *FIdx) loadIdxFile() {
	if exists(f.idxPath) {
		file, err := os.Open(f.idxPath)
		if err != nil {
			log.Fatalln("FWriter.loadIdxFile.文件打开失败", err)
		}
		var reader io.Reader
		if UseLz4 {
			reader = lz4.NewReader(file)
		} else {
			reader = bufio.NewReaderSize(file, 1024*1024*5)
		}
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

func (f *FIdx) SaveIdxFile() {
	file, err := os.OpenFile(f.idxPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalln("FWriter.SaveIdxFile.文件创建失败:", err)
	}
	fileInfo, _ := os.Stat(f.idxPath)
	var w IOWriter
	if UseLz4 {
		lz4w := lz4.NewWriter(file)
		lz4w.Apply(lz4.ChecksumOption(false), lz4.AppendOption(fileInfo.Size() > 4))
		w = lz4w
	} else {
		w = bufio.NewWriterSize(file, 1024*1024*5)
	}

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

func (f *FWriter) CreateIdxMeta() {
	if exists(f.FMeta.metaPath) {
		f.loadMeta()
		return
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()

	idx := uint64(0)
	offset := int64(0)
	lastOffset := int64(0)

	errPrint := func(err error) {
		if err.Error() != "EOF" {
			log.Println("FWriter.CreateIdxMeta err:", err, ",idx:", idx, ",offset:", offset)
		}
	}
	f.offsetList = []int64{0}
	for true {
		lastOffset = offset
		var d = make([]byte, LengthSide)
		_, err := f.readAt(d, HeadSize+offset)
		if err != nil {
			errPrint(err)
			break
		}
		length := f.toLenInt(d)
		f.addOffset(int(length + LengthSide + HeadSize))
		offset = offset + int64(length) + LengthSide + HeadSize
		idx++
	}
	first, _ := f.read(0)
	last, _ := f.read(int(idx - 1))

	f.setFirst(first)
	f.setLast(last)

	f.FMeta.num = idx
	f.FMeta.offset = uint64(lastOffset)
	f.flushMeta()

	log.Println("CreateIdxMeta.end,meta:", f.FMeta)
}

func (f *FWriter) loadIdxFromData() int {
	idx := 0
	offset := f.FIdx.offset()
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

	log.Println("FWriter["+f.path+"].LoadIndex: ", f.Count())
	if num > 0 {
		log.Println("FWriter["+f.path+"].LoadIndex: num:", num)
		f.SaveIdxFile()
	}
}
