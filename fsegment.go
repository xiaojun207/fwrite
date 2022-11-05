package fwrite

import (
	"bytes"
	"fmt"
	"github.com/pierrec/lz4/v4"
	"github.com/xiaojun207/fwrite/flz4"
	"github.com/xiaojun207/fwrite/utils"
	"log"
	"os"
)

type FSegment struct {
	FMeta
	SegLimit uint64
	path     string // path of segment file
	index    uint64 // first index of segment
	writer   IOWriter
}

func newSegment(index uint64, path string, segLimit uint64) *FSegment {
	name := path + "/" + segmentName(index) + ext
	return &FSegment{
		SegLimit: segLimit,
		index:    index,
		path:     name,
	}
}

func segmentName(index uint64) string {
	return fmt.Sprintf("%020d", index)
}

func (f *FSegment) full() bool {
	return f.FMeta.num+f.FMeta.bufNum >= f.SegLimit
}

func (f *FSegment) getWriter() IOWriter {
	if f.writer == nil {
		// Open the last segment for appending
		file, err := os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_RDWR, FilePerms)
		//file, err := os.OpenFile(f.path+f.currentFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatalln("FSegment.GetWriter.open, 文件创建失败", err)
		}
		fileInfo, _ := os.Stat(f.path)
		empty := fileInfo.Size() == 0
		if empty {
			f.FMeta.flushMeta(file)
		} else {
			f.FMeta.readMeta(file)
		}
		file.Seek(FMetaSize, 0)

		w := flz4.NewWriter(file)
		f.writer = w
		err = w.Writer.Apply(lz4.ChecksumOption(false), lz4.AppendOption(!empty))

		if err != nil {
			log.Println("FSegment.GetWriter.Apply.err:", err)
		}
	}
	return f.writer
}

func (f *FSegment) GetReader() (reader IOReader) {
	if !utils.Exists(f.path) {
		// 文件不存在，返回空reader
		log.Println("FSegment.GetReader is not exists:", f.path)
		return bytes.NewReader([]byte{})
	}
	file, err := os.Open(f.path)
	if err != nil {
		log.Fatalln("FSegment.GetReader, 文件打开失败", err)
	}
	n, err := f.FMeta.readMeta(file)
	reader = flz4.NewReader(file, int64(n))
	return
}

func (f *FSegment) write(d []byte) (n int, err error) {
	n, err = f.getWriter().Write(d)
	if err == nil {
		f.FMeta.fillToMeta(d[HeadSize+LengthSide:])
	}
	return
}

func (f *FSegment) flush() {
	if f.FMeta.bufNum > 0 {
		f.getWriter().Flush()

		// 写入文件头部
		file, err := os.OpenFile(f.path, os.O_CREATE|os.O_WRONLY, FilePerms)
		if err != nil {
			log.Fatalln("FSegment.flush.open, 文件创建失败", err)
		}
		//file.Seek(0, 0)
		f.FMeta.flushMeta(file)
	}
}

func (f *FSegment) reset() {
	f.writer = nil
}
