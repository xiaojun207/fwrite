package fwrite

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var path = "tmp/data"
var d []byte

func init() {
	td := map[string]string{
		"app": "user-service",
		"log": "2021-12-10 11:43:59,932 ERROR com.alibaba.cloud.nacos.registry.NacosServiceRegistry 75 nacos registry, manager register failed...NacosRegistration{nacosDiscoveryProperties=NacosDiscoveryProperties{serverAddr='192.168.2.43:8848', endpoint='', namespace='', watchDelay=30000",
	}
	d, _ = json.Marshal(td)
	//os.RemoveAll(path)
}

func TestTestFWriter(t *testing.T) {
	log.Println("d.len:", len(d))

	num := 100 * 10000
	start := time.Now()
	fwriter := New(path)
	log.Println("TestFWriterBuf New 耗时：", time.Since(start))
	start = time.Now()

	for i := 0; i < num; i++ {
		fwriter.Write(d[:20+i%200])
	}

	l := time.Since(start)
	log.Printf("TestTestFWriter 耗时：%s,平均：%f 条/s \n", l, float64(num*1000)/float64(l.Milliseconds()))
}

func TestFWriterBufGo(t *testing.T) {
	num := 10 * 10 * 10000
	start := time.Now()
	fwriter := New(path)
	log.Println("TestFWriterBuf New 耗时：", time.Since(start))
	start = time.Now()

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		f := func() {
			for j := 0; j < 10*10000; j++ {
				fwriter.WriteToBuf(d)
			}
			wg.Done()
		}
		go f()
	}
	wg.Wait()
	fwriter.Flush()

	l := time.Since(start)
	log.Printf("TestFWriterBufGo 耗时：%s,平均：%f 条/s \n", l, float64(num*1000)/float64(l.Milliseconds()))
}

func TestFWriterBuf(t *testing.T) {
	num := 10 * 10000
	start := time.Now()
	fwriter := New(path)
	fwriter.LoadIndex()
	log.Println("TestFWriterBuf New 耗时：", time.Since(start))
	start = time.Now()
	for i := 0; i < num; i++ {
		fwriter.WriteToBuf(d[:20+i%200])
	}
	fwriter.Flush()
	l := time.Since(start)

	log.Printf("TestFWriterBuf 耗时：%s,平均：%f 条/s \n", l, float64(num*1000)/float64(l.Milliseconds()))

	start = time.Now()
	fwriter.SaveIdxFile()
	log.Println("TestFWriterBuf SaveIdxFile 耗时：", time.Since(start))
	log.Println("count:", fwriter.Count())
}

func TestFileRead(t *testing.T) {
	start := time.Now()

	file, err := os.Open(path)
	if err != nil {
		log.Fatalln("文件打开失败", err)
	}

	log.Println("len:", len(d))
	var bl = make([]byte, 8)
	i, err := file.ReadAt(bl, 0)
	log.Println(i, err)

	length := binary.BigEndian.Uint64(bl)
	log.Println(length)

	log.Println("TestFileRead 耗时：", time.Since(start))
}

func TestFileWriter(t *testing.T) {
	start := time.Now()

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("文件打开失败", err)
	}
	for i := 0; i < 100*10000; i++ {
		file.Write(d)
	}

	log.Println("TestFileWriter 耗时：", time.Since(start))
}

func TestFileWriterBuf(t *testing.T) {
	start := time.Now()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("文件打开失败", err)
	}
	write := bufio.NewWriter(file)
	for i := 0; i < 100*10000; i++ {
		write.Write(d)
	}
	write.Flush()
	log.Println("TestFileWriterBuf 耗时：", time.Since(start))
}

func TestFileTest(t *testing.T) {
	bel := strconv.IsPrint('\007')
	fmt.Println(bel)
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
