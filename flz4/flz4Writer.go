package flz4

import (
	"github.com/pierrec/lz4/v4"
	"os"
)

func NewWriter(file *os.File) *FLz4 {
	f := &FLz4{
		r:      file,
		Writer: lz4.NewWriter(file),
	}
	return f
}
