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

## file

    * 00000001.f is the data, can not remove
    * 00000001.i is the idx file, if remove it, when read by idx will recreate.
    * meta.m is the meta info, if remove it, restart FWriter will recreate.
