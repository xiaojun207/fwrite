package fwrite

import (
	"log"
	"os"
	"testing"
)

func TestNewRead(t *testing.T) {
	path = os.Getenv("USER_HOME") + "/go/src/maize/tmp/maize/index/test-20221102"
	fw := New(path)
	segIndex := uint64(0)
	segFilter := func(index, num uint64, first, last []byte, offset uint64) bool {
		segIndex = index
		log.Println("segFilter, index:", index, ",num:", num, ",first:", first, ", last:", last, ",size:", offset)
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
		log.Println("rowFilter, index:", index, ",num:", 500000, ",first:", m2.first, ", last:", m2.last, ",size:", 0)
	}
	log.Println("TestNewRead.end:", m)
}
