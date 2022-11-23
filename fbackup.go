package fwrite

import (
	"log"
	"os"
)

func (f *FWriter) Backup(segIdx int, offset, length int64, writer IOWriter) (ret int64, err error) {
	path := f.segments[segIdx].path
	file, _ := os.Open(path)
	log.Println("Backup.path:", path)
	b := make([]byte, length)
	n, err := file.ReadAt(b, offset)
	writer.Write(b[0:n])
	writer.Flush()
	return file.Seek(0, 2)
}
