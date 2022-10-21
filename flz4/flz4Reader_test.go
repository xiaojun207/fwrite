package flz4

import (
	"log"
	"os"
	"testing"
)

func TestNew(t *testing.T) {
	path := "/Users/kdaxrobot/go/src/fwrite/"

	file, err := os.Open(path + "tmp/data/00000001.f")

	log.Println("TestNew.openRead:", err)
	reader := NewReader(file)

	offset := int64(0)
	var d = make([]byte, 6)
	n, err := reader.readAt(d, offset)
	log.Println("err:", err, ",n:", n, ",d:", d)

	log.Println("------------------------------------------------------------")
	var d2 = make([]byte, 6)
	n2, err2 := reader.readAt(d2, offset)
	log.Println("err2:", err2, ",n2:", n2, ",d2:", d2)

	log.Println("------------------------------------------------------------")
	log.Println("TestNew.end")
}
