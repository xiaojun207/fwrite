package fwrite

import (
	"encoding/json"
	"github.com/xiaojun207/fwrite/utils"
	"log"
	"os"
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
	utils.Task(t.Name()+"-Open", func() uint64 {
		fwriter = New(path)
		return fwriter.Count()
	})

	utils.Task(t.Name()+"-Write", func() uint64 {
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
		return uint64(num)
	})

	utils.Task(t.Name()+"-Read", func() uint64 {
		c := 0
		num := fwriter.Count()
		for i := num - 100; i < num; i++ {
			b, err := fwriter.Read(i)
			if err != nil {
				log.Println(i, "Read.err:", err, len(b))
			} else {
				//log.Println(i, "b:", b)
				//log.Println(i, "b:", string(b[2:]))
				c++
			}
		}
		log.Println("read.count:", c)
		return num
	})

	log.Println("count:", fwriter.Count())
	log.Printf("TestFWriterWrite Size: %v \n", fwriter.FileSize())
}

func TestNew(t *testing.T) {
	path := "tmp/maize/index/fluent.info-20221018"
	os.RemoveAll(path + "/meta.m")
	f := New(path)
	log.Println("count:", f.Count(), f.FirstData(), f.LastData())
}
