package flz4

import (
	"github.com/pierrec/lz4/v4"
	"log"
	"os"
)

func NewWriter(file *os.File, isNew bool) *FLz4 {
	f := &FLz4{
		readSeeker: file,
		Writer:     lz4.NewWriter(file),
	}
	err := f.Writer.Apply(lz4.ChecksumOption(false), lz4.ConcurrencyOption(-1), lz4.AppendOption(!isNew))
	//f.Writer.Apply(lz4.LegacyOption(true))

	if err != nil {
		log.Println("FSegment.GetWriter.Apply.err:", err)
	}
	return f
}
