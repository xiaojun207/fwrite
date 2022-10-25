package main

import (
	"encoding/json"
	"github.com/xiaojun207/fwrite"
	"github.com/xiaojun207/fwrite/utils"
	"github.com/xiaojun207/go-base-utils/math"
	"log"
	"math/rand"
	"time"
)

var path = "tmp/data"
var d []byte
var num = 2

func init() {
	td := map[string]string{
		"id:": "00000",
		"app": "user-service",
		"log": "2021-12-10 11:43:59,932 ERROR com.alibaba.cloud.nacos.registry.NacosServiceRegistry 75 nacos registry, manager register failed...NacosRegistration{nacosDiscoveryProperties=NacosDiscoveryProperties{serverAddr='192.168.2.43:8848', endpoint='', namespace='', watchDelay=30000",
	}
	d, _ = json.Marshal(td)
	//os.RemoveAll(path)
}

func main() {
	var fwriter *fwrite.FWriter

	utils.Task("Demo"+"-Open", func() uint64 {
		fwriter = fwrite.New(path)
		return fwriter.Count()
	})

	time.Sleep(time.Second)
	utils.Task("Demo"+"-Write", func() uint64 {
		for i := 0; i < num; i++ {
			b := d
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

	time.Sleep(time.Second)
	utils.Task("Demo"+"-ForEach", func() uint64 {
		count := 0
		err := fwriter.ForEach(func(d []byte) bool {
			count++
			return true
		})
		if err != nil {
			log.Println("Demo-ForEach.err:", err)
		}
		return uint64(count)
	})

	time.Sleep(time.Second)
	utils.Task("Demo"+"-Foreach", func() uint64 {
		count := 0
		_, err := fwriter.Foreach(func(idx uint64, offset int64, length fwrite.LenInt, d []byte) bool {
			count++
			return true
		})
		if err != nil {
			log.Println("Demo-Foreach.err:", err)
		}
		return uint64(count)
	})

	time.Sleep(time.Second)
	utils.Task("Demo"+"-LoadIdx", fwriter.LoadIdx)

	time.Sleep(time.Second)
	utils.Task("Demo"+"-Search", func() uint64 {
		res, err := fwriter.Search(func(d []byte) bool {
			return true
		})
		log.Println("Demo-Search.err:", err)
		return uint64(len(res))
	})

	time.Sleep(time.Second)
	utils.Task("Demo"+"-Read", func() uint64 {
		c := 0
		n := fwriter.Count()
		log.Println("Demo-Read.n:", n)
		start := math.Max(n-1, 0)
		for i := start; i < n; i++ {
			b, err := fwriter.Read(i)
			if err != nil {
				log.Println("i:", i, "Read.err:", err, len(b))
				break
			} else {
				//log.Println("i:", i, "b:", b)
				//log.Println("i:", i, "b:", string(b))
				c++
			}
		}
		return uint64(c)
	})

	time.Sleep(time.Second)
	utils.Task("Demo"+"-Rand", func() uint64 {
		c := 0
		n := fwriter.Count()
		log.Println("Demo-Rand.n:", n)
		for i := 0; i < 10; i++ {
			idx := rand.Int63n(int64(n))
			tl := time.Now()
			b, err := fwriter.Read(uint64(idx))
			if err != nil {
				log.Println("i:", i, "Rand.Read.err:", err, len(b))
				break
			} else {
				//log.Println("i:", i, "b:", b)
				//log.Println("i:", i, "b:", string(b))
				c++
			}
			log.Println("Rand.Read,idx:", idx, ",耗时：", time.Since(tl))
		}
		return uint64(c)
	})

	time.Sleep(time.Second)
	log.Println("fwriter.FMeta:", fwriter.FMeta)
	log.Println("count:", fwriter.Count())
	log.Printf("TestFWriterWrite Size: %v \n", fwriter.FileSize())
	time.Sleep(time.Second * 10)
}
