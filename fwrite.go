package fwrite

import (
	"bufio"
	"log"
	"os"
	"sync"
)

type FWriter struct {
	path      string
	file      *os.File
	bufWriter *bufio.Writer
	mutex     sync.RWMutex
}

func New(path string) *FWriter {
	f := &FWriter{
		path: path,
	}
	f.open()
	return f
}

func (f *FWriter) open() {
	file, err := os.OpenFile(f.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("文件打开失败", err)
	}
	f.file = file
	//bufio.NewReadWriter(f.file, f.file)
	f.bufWriter = bufio.NewWriter(f.file)
}

func (f *FWriter) Path() string {
	return f.path
}

func (f *FWriter) File() *os.File {
	return f.file
}

func (f *FWriter) Write(d []byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.file.Write(d)
}
func (f *FWriter) BatchWrite(arr [][]byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	count := 0
	for _, d := range arr {
		l, err := f.file.Write(d)
		if err != nil {
			log.Fatalln("BatchWrite.err:", err)
		}
		count = count + l
	}
	return count, nil
}

func (f *FWriter) Read(d []byte) (int, error) {
	return f.file.Read(d)
}

func (f *FWriter) WriteAt(d []byte, offset int64) (int, error) {
	return f.file.WriteAt(d, offset)
}

func (f *FWriter) ReadAt(d []byte, offset int64) (int, error) {
	return f.file.ReadAt(d, offset)
}

func (f *FWriter) WriteToBuf(d []byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.bufWriter.Write(d)
}

func (f *FWriter) BatchWriteToBuf(arr [][]byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	count := 0
	for _, d := range arr {
		l, err := f.bufWriter.Write(d)
		if err != nil {
			log.Fatalln("BatchWrite.err:", err)
		}
		count = count + l
	}
	return count, nil
}

func (f *FWriter) Flush() {
	f.bufWriter.Flush()
}
