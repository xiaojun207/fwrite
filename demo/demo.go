package main

import (
	"encoding/binary"
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
var num = 200 * 10000
var fwriter *fwrite.FWriter

func init() {
	td := map[string]string{
		"id:": "00000",
		"app": "user-service",
		"log": "2021-12-10 11:43:59,932 ERROR com.alibaba.cloud.nacos.registry.NacosServiceRegistry 75 nacos registry, manager register failed...NacosRegistration{nacosDiscoveryProperties=NacosDiscoveryProperties{serverAddr='192.168.2.43:8848', endpoint='', namespace='', watchDelay=30000",
	}

	d = binary.BigEndian.AppendUint64(d, uint64(time.Now().Unix()))
	d = binary.BigEndian.AppendUint64(d, uint64(time.Now().Unix()))

	d2, _ := json.Marshal(td)
	d = append(d, d2...)
	//os.RemoveAll(path)
}

func write() {
	first_t1 := binary.BigEndian.Uint64(d[0:8])
	first_t2 := binary.BigEndian.Uint64(d[8:16])
	log.Println("d:", d[0:16], first_t1, first_t2)
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
}

func read() {

	time.Sleep(time.Second)
	utils.Task("Demo"+"-Foreach", func() uint64 {
		count := 0
		segFilter := func(index, num uint64, first, last []byte, offset uint64) bool {
			first_t2 := binary.BigEndian.Uint64(first[8:16])
			last_t2 := binary.BigEndian.Uint64(last[8:16])
			log.Println("segFilter, firstT[", first_t2, "]", ", last[", last_t2, "],first:", first, ",last:", last)
			return true
		}
		rowFilter := func(idx uint64, offset int64, length fwrite.LenInt, d []byte) bool {
			//log.Println("rowFilter,idx:", idx, ",d:", d[8:16])
			count++
			return true
		}
		_, err := fwriter.Foreach(segFilter, rowFilter)
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
		start := math.Max(n-10, 0)
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
		for i := 0; i < 5; i++ {
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
}

func main() {
	defer func() {
		err := recover()
		if err != nil {
			log.Println("main.defer,err:", err)
		}
	}()
	utils.Task("Demo"+"-Open", func() uint64 {
		fwriter = fwrite.New(path)
		return fwriter.Count()
	})

	time.Sleep(time.Second)

	write()
	//
	read()

	time.Sleep(time.Second)
	log.Println("fwriter.FMeta:", fwriter.FMeta.String())
	log.Println("count:", fwriter.Count())
	log.Printf("TestFWriterWrite Size: %v \n", fwriter.FileSize())
	time.Sleep(time.Second * 10)
}
