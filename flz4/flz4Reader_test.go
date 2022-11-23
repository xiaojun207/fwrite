package flz4

import (
	"encoding/binary"
	"errors"
	"github.com/pierrec/lz4/v4"
	"github.com/xiaojun207/fwrite/utils"
	"github.com/xiaojun207/go-base-utils/sort"
	"io"
	"log"
	"os"
	"testing"
	"time"
)

func TestNew(t *testing.T) {

	path := os.Getenv("USER_HOME") + "/go/src/fwrite/tmp/data/00000000000000000000.f"

	file, err := os.Open(path)

	log.Println("TestNew.openRead:", err)
	reader := NewReader(file, 48)

	offset := int64(0)
	var d = make([]byte, 6)
	n, err := reader.readAt(d, offset)
	log.Println("err:", err, ",n:", n, ",d:", d)

	log.Println("------------------------------------------------------------")
	var d2 = make([]byte, 6)
	n2, err2 := reader.readAt(d2, offset)
	log.Println("err2:", err2, ",n2:", n2, ",d2:", d2)

	log.Println("------------------------------------------------------------")
	log.Println("TestNew.end")
}

func TestReadAll(t *testing.T) {
	tl := time.Now()
	path := "tmp/test.f"
	os.RemoveAll(path)
	file1, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Println("FLz4.WriteToFile.err:", err)
		return
	}
	writer := lz4.NewWriter(file1)
	writerHandler := func(size int) {
		ret, _ := file1.Seek(0, 1)
		log.Println("writer.OnBlockDone,ret:", ret, ",size:", size)
	}
	writer.Apply(lz4.OnBlockDoneOption(writerHandler))

	buf := []byte{255, 255, 255, 255, 254, 254, 254, 254}
	writer.Write(buf)
	writer.Flush()

	buf2 := []byte{255, 255, 255, 255, 254, 254, 254, 254, 254, 254, 254, 254}
	writer.Write(buf2)
	writer.Flush()

	file, err := os.Open(path)
	if err != nil {
		log.Println("Read.open.err:", err)
	}
	reader := lz4.NewReader(file)
	offsetMap := map[int64]int64{}
	offset := 0
	handler := func(size int) {
		ret, _ := file.Seek(0, 1)
		offset += size
		offsetMap[ret] = int64(offset)
		log.Println("OnBlockDone,ret:", ret, ",size:", size, ",offset:", offset)
	}
	reader.Apply(lz4.OnBlockDoneOption(handler))

	n, err := io.CopyN(io.Discard, reader, 5)
	log.Println("copyN,n:", n, ",err:", err)

	file.Seek(0, 0)
	reader.Reset(file)
	log.Println("Read.All")
	//n, err = reader.WriteTo(io.Discard)
	n, err = io.Copy(io.Discard, reader)
	log.Println("Copy,n:", n, ",err:", err)
	log.Println("ReadAll.end")

	log.Println("end,offsetMap:", len(offsetMap), ",耗时:", time.Since(tl))
}

func TestRead(t *testing.T) {
	tl := time.Now()
	log.Println("start")
	path := "tmp/maize/index/nacos-20221020/00000001.f"
	file, err := os.Open(path)
	if err != nil {
		log.Println("Read.open.err:", err)
	}
	reader := lz4.NewReader(file)
	offsetMap := map[int64]int64{}

	count := int64(0)
	handler := func(size int) {
		ret, _ := file.Seek(0, 1)
		offsetMap[ret] = count
		log.Println("OnBlockDone:", size)
	}
	reader.Apply(lz4.OnBlockDoneOption(handler))

	idx := 0
	for true {
		//var buf = make([]byte, 9)
		log.Println("ReadN")
		n, err := io.CopyN(io.Discard, reader, 100)
		if err != nil {
			log.Println("Discard.err:", err, ",n:", n)
			break
		}

		log.Println("ReadN.end")
		count += n
		idx++

		if idx%100000 == 0 {
			log.Println("offsetMap:", len(offsetMap), ",count:", count)
		}
	}
	log.Println("end,offsetMap:", len(offsetMap), ",耗时:", time.Since(tl), ", count:", count)

	tl = time.Now()
	var d []byte
	for i, _ := range offsetMap {
		d = binary.BigEndian.AppendUint64(d, uint64(i))
	}
	os.WriteFile("offset.idx", d, 0666)

	log.Println("end,save,耗时:", time.Since(tl), ", count:", count)
	log.Println("offsetMap:", offsetMap)

	for i := 1; i < 100; i++ {
		file.Seek(0, 0)
		reader.Reset(file)
		io.CopyN(io.Discard, reader, 1)
		io.CopyN(io.Discard, reader, int64(60*i))
		ret, _ := file.Seek(0, 1)
		log.Println("data.offset:", i*60, ",lz4.ret:", ret)
	}
}

func TestLz4ReadAt(t *testing.T) {
	d, _ := os.ReadFile("offset.idx")
	var offsetList []uint64
	for i := 0; i < len(d)/8; i++ {
		offsetList = append(offsetList, binary.BigEndian.Uint64(d[i*8:i*8+8]))
	}
	sort.Sort[uint64](offsetList)
	log.Println("offsetList:", offsetList[0:100])

}

func TestReadHead(t *testing.T) {
	path := os.Getenv("USER_HOME") + "/go/src/fwrite/tmp/data/00000000000000000000.f"
	fr, _ := os.Open(path)
	header := make([]byte, 48)
	n, err := fr.Read(header)
	log.Println("header,n:", n, ",err:", err, "readLz4.header:", header)
	r := lz4.NewReader(fr)

	buf := make([]byte, 10)
	n, err = r.Read(buf)
	log.Println("buf,n:", n, ",err:", err, ",buf:", buf)

}

func TestSeek(t *testing.T) {
	path := os.Getenv("USER_HOME") + "/go/src/fwrite/tmp/data/00000000000000000000.f"
	fr, _ := os.Open(path)
	last, _ := fr.Seek(0, 2)
	log.Println("last:", last)
	buf := make([]byte, 5)
	n, err := fr.ReadAt(buf, last+5)
	log.Println("ReadAt,n:", n, ",err:", err, ",buf:", buf)
}

func TestNewReader(t *testing.T) {
	path := os.Getenv("USER_HOME") + "/go/src/fwrite/tmp/data/00000000000000000000.f"
	file, err := os.Open(path)
	log.Println("TestNewReader.openRead:", err)

	file.Seek(48, 0)
	reader := NewReader(file, 48)

	utils.Task("TestNewReader", "M", func() uint64 {
		r := Receiver{}
		r.OnRead = func(b []byte) {
			//log.Println("b:", b[0:5], len(b))
		}

		//n, err := reader.WriteTo(readSeeker)
		n, err := io.Copy(r, reader)
		if err != nil {
			log.Println("err:", err)
		}
		return uint64(n / 1024 / 1024)
	})
}

func copyBuffer(dst io.Writer, src io.Reader, buf []byte) (written int64, err error) {
	// If the reader has a WriteTo method, use it to do the copy.
	// Avoids an allocation and a copy.
	if wt, ok := src.(io.WriterTo); ok {
		return wt.WriteTo(dst)
	}
	// Similarly, if the writer has a ReadFrom method, use it to do the copy.
	if rt, ok := dst.(io.ReaderFrom); ok {
		return rt.ReadFrom(src)
	}
	if buf == nil {
		size := 32 * 1024
		if l, ok := src.(*io.LimitedReader); ok && int64(size) > l.N {
			if l.N < 1 {
				size = 1
			} else {
				size = int(l.N)
			}
		}
		buf = make([]byte, size)
	}
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errors.New("invalid write result")
				}
			}
			written += int64(nw)
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}
