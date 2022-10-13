# github.com/xiaojun207/fwrite
This is fast writer

## use
```
    fwriter := fwrite.New(path)
	for i := 0; i < 100*10000; i++ {
		fwriter.Write(d)
	}
	fwriter.Flush()
	
	for i := 0; i < 100*10000; i++ {
		d,err := fwriter.Read(i)
		log.Println("err:", err, "d:", d)
	}
	
	// 
	fwriter.SaveIdxFile()
	
```
