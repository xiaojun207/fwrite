package utils

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"github.com/pierrec/lz4/v4"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

const (
	Layout = "2006-01-02 15:04:05.000000000"
)

func TimeToStr(mese int64) string {
	return time.UnixMilli(mese).Format(Layout)
}

func PrintProgress[T float64 | float32 | int64 | uint64 | int](progress, total T) {
	Printf("\r进度 %.2f%%", float64(progress*10000/total)/100)
}

func Printf(format string, a ...any) {
	fmt.Printf(format, a...)
}

func Println(s ...any) {
	fmt.Println(time.Now().Format(Layout), s)
}

func PrintlnError(err error, a ...any) {
	if err.Error() != "EOF" {
		log.Println(err, fmt.Sprint(a...))
	}
}

func Task(name, unit string, f func() uint64) {
	time.Sleep(time.Millisecond * 300)
	Printf("..........................................[%s].................................................................\n", name)
	Printf("%v task[%s]start...\n", time.Now().Format(Layout), name)
	time.Sleep(time.Millisecond * 300)
	t := time.Now()
	n := f()
	tl := time.Since(t)
	time.Sleep(time.Millisecond * 300)
	Printf("%v task[%s]end，耗时：%v，平均:%.f%s/s，总计:%d%s \n", time.Now().Format(Layout), name, tl, float64(n*1000*1000*1000)/float64(tl.Nanoseconds()), unit, n, unit)
	time.Sleep(time.Millisecond * 300)
}

// Exists returns whether the given file or directory exists or not
func Exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return !os.IsNotExist(err)
}

func Size(path string) int64 {
	f, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return f.Size()
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func ZlibCompressFile(src, dest string) {
	file, err := os.Open(src)
	checkErr(err)
	d, err := io.ReadAll(file)
	if err != nil {
		log.Fatal(err)
	}
	d = ZlibCompress(d)
	os.WriteFile(dest, d, 0666)
	if err != nil {
		log.Fatal(err)
	}
}

// 进行zlib压缩
func ZlibCompress(src []byte) []byte {
	var in bytes.Buffer
	w, _ := zlib.NewWriterLevel(&in, zlib.BestCompression)
	w.Write(src)
	w.Flush()
	w.Close()
	return in.Bytes()
}

// 进行zlib解压缩
func ZlibUnCompress(compressSrc []byte) []byte {
	b := bytes.NewReader(compressSrc)

	r, err := zlib.NewReader(b)
	if err != nil {
		log.Println("ZlibUnCompress.err:", err)
	}
	io.ReadAll(r)
	var out bytes.Buffer
	io.Copy(&out, r)
	return out.Bytes()
}

func Uint16ToByte(i uint16) []byte {
	arr := make([]byte, 2)
	binary.BigEndian.PutUint16(arr, i)
	return arr
}

func Uint32ToByte(i uint32) []byte {
	arr := make([]byte, 4)
	binary.BigEndian.PutUint32(arr, i)
	return arr
}

func Uint64ToByte(i uint64) []byte {
	arr := make([]byte, 8)
	binary.BigEndian.PutUint64(arr, i)
	return arr
}

func ByteToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func ByteToUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func ByteToUint16(b []byte) uint16 {
	return binary.BigEndian.Uint16(b)
}

func Lz4(d []byte) []byte {
	buf := make([]byte, lz4.CompressBlockBound(len(d)))
	var c lz4.Compressor
	n, err := c.CompressBlock(d, buf)
	if err != nil {
		fmt.Println(err)
	}
	if n >= len(d) {
		fmt.Printf("`%v` is not compressible", d)
	}
	buf = buf[:n] // compressed data
	return buf
}

func UnLz4(d []byte) []byte {
	out := make([]byte, 10*len(d))
	n, err := lz4.UncompressBlock(d, out)
	if err != nil {
		fmt.Println(err)
	}
	out = out[:n] // uncompressed data
	return out
}

func Indexes(s, sep []byte) (res []int) {
	if len(sep) == 0 {
		return
	}
	idx := 0
	for {
		i := bytes.Index(s, sep)
		if i == -1 {
			return
		}
		res = append(res, idx+i)
		s = s[i+len(sep):]
		idx += i + len(sep)
	}
	return
}

func StrIndexes(s, sep string) (res []int) {
	if len(sep) == 0 {
		return
	}
	idx := 0
	for {
		i := strings.Index(s, sep)
		if i == -1 {
			return
		}
		res = append(res, idx+i)
		s = s[i+len(sep):]
		idx += i + len(sep)
	}
	return
}
