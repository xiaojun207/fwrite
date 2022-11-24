package fwrite

import (
	"bytes"
	"encoding/binary"
	"github.com/edsrzf/mmap-go"
	"github.com/pierrec/lz4/v4"
	"github.com/xiaojun207/fwrite/utils"
	"io"
	"log"
	"os"
	"testing"
	"time"
)

func TestNewReadByte(t *testing.T) {
	buf := []byte{4, 34, 77, 24, 100, 112, 185, 16, 0, 0, 128, 22, 9, 181, 61, 115, 64, 16, 43, 22, 9, 143, 57, 4, 64, 16, 1}
	r := lz4.NewReader(bytes.NewReader(buf))
	var out bytes.Buffer
	io.Copy(&out, r)
	log.Println("out:", out.Bytes())
}

func TestMmapSearchName(t *testing.T) {
	//2022/11/15 11:08:57 segFilter, index: 0 ,first 1587957818672025601 ,segFilter.last 1587999619156021291
	//2022/11/15 11:08:57 segFilter, index: 0 ,num: 500000 ,first: [22 9 143 57 4 64 16 1 0 0 1 132 58 201 148 39] , last: [22 9 181 61 115 64 16 43 0 0 1 132 59 80 148 49] ,size: 119799261
	//2022/11/15 17:00:58 rowFilter, index: 0 ,num: 500000 ,first: 1587957818672025601 ,last: 1587999619156021291 , last: [22 9 181 61 115 64 16 43 0 0 1 132 59 80 148 49] ,size: 0

	var q []byte
	q = binary.BigEndian.AppendUint64(q, uint64(1668521876))
	q = []byte("java")
	var q2 []byte
	q2 = binary.BigEndian.AppendUint64(q2, uint64(1668521878))
	log.Println("query:", q, ",q2:", q2)
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	log.Println("wbuf1:", buf.Bytes())
	w.Write(q)
	//w.Write(q)
	log.Println("wbuf2:", buf.Bytes())
	w.Flush()
	log.Println("wbuf3:", buf.Bytes())
	w.Write(q2)
	w.Flush()
	log.Println("wbuf5:", buf.Bytes())
	//wbuf5:[4 34 77 24 100 112 185 8  0 0 128 22 9 181 61 115 64 16 43 8 0 0 128 22 9 143 57 4 64 16 1]
	//wbuf: [4 34 77 24 100 112 185 16 0 0 128 22 9 181 61 115 64 16 43 22 9 181 61 115 64 16 43 8 0 0 128 22 9 143 57 4 64 16 1
	//wbuf: [4 34 77 24 100 112 185 8 0 0 128 22 9 143 57 4 64 16 1]

	tl := time.Now()
	MmapSearchName(q)
	log.Println("耗时：", time.Since(tl))
}

func MmapSearchName(query []byte) {
	path = os.Getenv("USER_HOME") + "/go/src/maize/tmp/maize/index/test-20221121"
	//path = os.Getenv("USER_HOME") + "/go/src/fwrite/tmp/data"
	path = path + "/00000000000000000000.f"
	file, err := os.Open(path)
	if err != nil {
		log.Println("MmapSearchName.loadIdxMMap.文件打开失败", err)
		return
	}
	fileMMap, err := mmap.Map(file, mmap.RDONLY, 0)

	idxes := utils.Indexes(fileMMap, query)
	log.Println("query:", query, ",count:", len(idxes))
	for _, idx := range idxes {
		log.Println("idx:", idx, ",d:", fileMMap[idx:idx+18])
	}
	log.Println("index:", idxes)
}

func TestNewRead(t *testing.T) {
	path = os.Getenv("USER_HOME") + "/go/src/fwrite/tmp/data"

	fw := New(path)

	log.Println("fw.first", binary.BigEndian.Uint64(fw.first[0:8]))

	segIndex := uint64(0)
	segFilter := func(index, num uint64, first, last []byte, offset uint64) bool {
		segIndex = index
		log.Println("segFilter, index:", index, ",num:", num, ",first", binary.BigEndian.Uint64(first[0:8]), ",first:", first, ",.last", binary.BigEndian.Uint64(last[0:8]), ", last:", last, ",size:", offset)
		return true
	}
	type meta struct {
		first, last []byte
	}
	m := map[uint64]meta{}
	rowFilter := func(idx uint64, offset int64, length LenInt, d []byte) bool {
		s, ok := m[segIndex]
		if ok {
			s.last = d[0:16]
		} else {
			s = meta{
				first: d[0:16],
				last:  d[0:16],
			}
		}
		m[segIndex] = s
		return true
	}
	fw.Foreach(segFilter, rowFilter)
	for index, m2 := range m {
		log.Println("rowFilter, index:", index, ",num:", 500000, ",first:", binary.BigEndian.Uint64(m2.first[0:8]), ",last:", binary.BigEndian.Uint64(m2.last[0:8]), ", last:", m2.last, ",size:", 0)
	}
	log.Println("TestNewRead.end:", m)
}

func TestNewReadTime(t *testing.T) {
	path = os.Getenv("USER_HOME") + "/go/src/maize/tmp/maize/index/test-20221123"
	//path = os.Getenv("USER_HOME") + "/go/src/fwrite/tmp/data"
	fw := New(path)

	log.Println("fw.segments:", len(fw.segments))
	log.Println("fw.first:", binary.BigEndian.Uint64(fw.first[0:8]), ",last:", binary.BigEndian.Uint64(fw.last[0:8]))

	segFilter := func(index, num uint64, first, last []byte, offset uint64) bool {
		//id := binary.BigEndian.Uint64(first[0:8])
		//time_ := binary.BigEndian.Uint64(first[8:16])
		//log.Println("segFilter, index:", index, ",num:", num, ",id:", id, ", time:", time_, ",size:", offset)
		return index < 58358
		//return true
	}

	size := 0
	count := 0
	rowFilter := func(idx uint64, offset int64, length LenInt, d []byte) bool {
		if idx%10000 == 0 {
			//id := binary.BigEndian.Uint64(d[0:8])
			//time_ := binary.BigEndian.Uint64(d[8:16])
			//log.Println("d:", id, time_, string(d))
			//log.Println("idx:", idx, ",offset:", offset, ",length", length, ",d:", len(d))
		}
		size += len(d) + 5
		count++
		return true
	}
	tl := time.Now()

	idx, err := fw.Foreach(segFilter, rowFilter)
	log.Println("耗时:", time.Since(tl), ",数量：", fw.Count(), ",count:", count, ",size:", size, ",getSize:", fw.Size())
	log.Println("res:", idx, ",err:", err)
}

func TestNewReadSegment(t *testing.T) {
	path = os.Getenv("USER_HOME") + "/go/src/maize/tmp/maize/index/test-20221123"
	//path = os.Getenv("USER_HOME") + "/go/src/fwrite/tmp/data"
	fw := New(path)

	log.Println("fw.segments:", len(fw.segments))
	log.Println("fw.first:", binary.BigEndian.Uint64(fw.first[0:8]), ",last:", binary.BigEndian.Uint64(fw.last[0:8]))

	size := 0
	count := 0
	rowFilter := func(idx uint64, offset int64, length LenInt, d []byte) bool {
		if idx > fw.segLimit {
			id := binary.BigEndian.Uint64(d[0:8])
			time_ := binary.BigEndian.Uint64(d[8:16])
			log.Println("d:", id, time_, string(d[16:]))
			log.Println("idx:", idx, ",offset:", offset, ",length", length, ",d:", len(d))
		}
		size += len(d) + 5
		count++
		return true
	}
	tl := time.Now()

	idx, err := fw.ReadSegment(0, rowFilter)
	log.Println("耗时:", time.Since(tl), ",数量：", fw.Count(), ",count:", count, ",size:", size, ",getSize:", fw.Size())
	log.Println("res:", idx, ",err:", err)
}

func TestSearchRead(t *testing.T) {
	path = os.Getenv("USER_HOME") + "/go/src/maize/tmp/maize/index/test-20221123"
	fw := New(path)

	utils.Task("TestSearchRead", "条", func() uint64 {
		count := fw.FReader.Search([]byte("java"))
		log.Println("Search.Result:", count)
		return uint64(fw.Count())
	})

}
