package fwrite

import (
	"encoding/json"
	"log"
	"testing"
)

var path = "tmp/data"
var d []byte
var num = 30 * 10000

func init() {
	td := map[string]string{
		"id:": "00000",
		"app": "user-service",
		"log": "2021-12-10 11:43:59,932 ERROR com.alibaba.cloud.nacos.registry.NacosServiceRegistry 75 nacos registry, manager register failed...NacosRegistration{nacosDiscoveryProperties=NacosDiscoveryProperties{serverAddr='192.168.2.43:8848', endpoint='', namespace='', watchDelay=30000",
	}
	d, _ = json.Marshal(td)
	//os.RemoveAll(path)
}

func getTestData(i int) []byte {
	//b := []byte(strconv.Itoa(i))
	//old := []byte{48, 48, 48, 48, 48}
	return d
}

func TestTestFWriter(t *testing.T) {

	var fwriter *FWriter
	Task(t.Name()+"-Open", func() int64 {
		fwriter = New(path)
		fwriter.LoadIndex()
		return int64(fwriter.Count())
	})

	Task(t.Name()+"-Write", func() int64 {
		for i := 0; i < num; i++ {
			b := getTestData(i)
			nn, err := fwriter.Write(b)
			if err != nil {
				log.Println("Write.err:", err, nn)
			}
			if i > 0 && i%10000 == 0 {
				fwriter.Flush()
			}
		}
		fwriter.Flush()
		return int64(num)
	})

	Task(t.Name()+"-Read", func() int64 {
		c := 0
		num := fwriter.Count()
		for i := num - 100; i < num; i++ {
			b, err := fwriter.Read(uint(i))
			if err != nil {
				log.Println(i, "Read.err:", err, len(b))
			} else {
				//log.Println(i, "b:", b)
				//log.Println(i, "b:", string(b[2:]))
				c++
			}
		}
		log.Println("read.count:", c)
		return int64(num)
	})

	Task(t.Name()+"-SaveIdxFile", func() int64 {
		// 500*10000*8/1024/1024
		fwriter.SaveIdxFile()
		return int64(num)
	})

	log.Println("count:", fwriter.Count())
	log.Printf("TestFWriterWrite Size: %v \n", fwriter.FileSize())
}
