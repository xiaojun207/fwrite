package fwrite

import (
	"bytes"
	"encoding/binary"
	"log"
	"strings"
	"testing"
	"time"
)

func TestZlibCompress(t *testing.T) {
	ZlibCompressFile("tmp/logs/common-error.log", "tmp/logs/common-error.log.z")
	ZlibCompressFile("tmp/logs/common-error_2021-10-14_1.log", "tmp/logs/common-error_2021-10-14_1.log.z")
	ZlibCompressFile("tmp/logs/common-info.log", "tmp/logs/common-info.log.z")
	ZlibCompressFile("tmp/logs/common-info_2021-10-14_1.log", "tmp/logs/common-info_2021-10-14_1.log.z")
}

func TestByteTest(t *testing.T) {
	num := 10000 * 10000

	timeFrom := uint64(1663804830254)
	timeEnd := uint64(1663891199297)
	var arr []byte

	start := make([]byte, 8)
	binary.BigEndian.PutUint64(start, timeFrom)
	end := make([]byte, 8)
	binary.BigEndian.PutUint64(end, timeEnd)

	arr = append(arr, start...)
	arr = append(arr, end...)

	s := time.Now()
	for i := 0; i < num; i++ {
		timestamp := d[8:16]
		if bytes.Compare(timestamp, start) < 0 || bytes.Compare(timestamp, end) >= 0 {
			continue
		}
	}
	log.Println("[]byte.l", time.Since(s))

	s = time.Now()
	for i := 0; i < num; i++ {
		timestamp := binary.BigEndian.Uint64(d[8:16])
		if timestamp < timeFrom || timestamp >= timeEnd {
			continue
		}
	}
	log.Println("uint64.l", time.Since(s))
}

func TestQueryByteTest(t *testing.T) {
	num := 10000 * 10000

	query := "This is a text"
	byteQuery := []byte(query)
	var arr []byte
	arr = append(arr, []byte("tool test2json -t /private/var/Process finished with the exit code 0"+query+"estQueryByteTest_in_github_")...)

	s := time.Now()
	for i := 0; i < num; i++ {
		bytes.Contains(arr, byteQuery)
	}
	log.Println("[]byte.l:", time.Since(s))

	s = time.Now()
	for i := 0; i < num; i++ {
		text := string(arr)
		strings.Contains(text, query)
	}
	log.Println("string.l:", time.Since(s))
}
