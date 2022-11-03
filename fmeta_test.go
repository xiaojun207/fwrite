package fwrite

import (
	"bytes"
	"log"
	"testing"
)

func TestFirstEmpty(t *testing.T) {
	b := make([]byte, FMetaDataSize)
	r := bytes.Equal(b, emptyMetaData)
	log.Println("TestFirstEmpty:", r)
}
