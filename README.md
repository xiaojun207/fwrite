# fwrite
This is fast writer

## use
```
    fwriter := New(path)
	for i := 0; i < 100*10000; i++ {
		fwriter.WriteToBuf(d)
	}
	fwriter.Flush()
```
