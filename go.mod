module github.com/xiaojun207/fwrite

go 1.19

replace github.com/pierrec/lz4/v4 v4.1.17 => github.com/xiaojun207/lz4/v4 v4.1.18

require (
	github.com/edsrzf/mmap-go v1.1.0
	github.com/pierrec/lz4/v4 v4.1.17
	github.com/xiaojun207/go-base-utils v0.2.5
)

require golang.org/x/sys v0.1.0 // indirect
