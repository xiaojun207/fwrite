package fwrite

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
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
}

func TestTestFWriter(t *testing.T) {
	start := time.Now()

	fwriter := New(path)
	for i := 0; i < 100*10000; i++ {
		fwriter.Write(d)
	}
	log.Println("TestTestFWriter 耗时：", time.Since(start))
}

func TestFWriterBufGo(t *testing.T) {
	start := time.Now()

	fwriter := New(path)
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		f := func() {
			for j := 0; j < 10000; j++ {
				fwriter.WriteToBuf(d)
			}
			wg.Done()
		}
		go f()
	}
	wg.Wait()
	fwriter.Flush()

	log.Println("TestFWriterBufGo 耗时：", time.Since(start))
}

func TestFWriterBuf(t *testing.T) {
	start := time.Now()
	fwriter := New(path)
	log.Println("TestFWriterBuf New 耗时：", time.Since(start))

	start = time.Now()
	for i := 0; i < 1*10000; i++ {
		fwriter.WriteToBuf(d)
	}
	fwriter.Flush()

	log.Println("TestFWriterBuf 耗时：", time.Since(start))
	log.Println(len(fwriter.indexList))

	start = time.Now()
	d, err := fwriter.Read(10000)
	if err != nil {
		log.Println("TestFWriterBuf.err:", err)
	}
	log.Println("TestFWriterBuf read 耗时：", time.Since(start), string(d))
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
