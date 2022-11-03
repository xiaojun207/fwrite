package fwrite

import (
	"log"
	"testing"
)

func TestNewName(t *testing.T) {
	log.Println(segmentName(0))
	log.Println(segmentName(1))
	log.Println(segmentName(2))
}
