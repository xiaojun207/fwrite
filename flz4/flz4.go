package flz4

import (
	"github.com/pierrec/lz4/v4"
	"io"
	"log"
	"os"
)

func WriteToFile(fileName string, data []byte) {
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Println("FLz4.WriteToFile.err:", err)
		return
	}
	writer := lz4.NewWriter(file)
	writer.Write(data)
	writer.Flush()
}

func ReadFromFile(fileName string) ([]byte, error) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Println("FLz4.ReadFromFile.err:", err)
		return nil, err
	}
	r := lz4.NewReader(file)
	return io.ReadAll(r)
}

func exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return !os.IsNotExist(err)
}
