package fwrite

import "io"

type eofReader struct{}

func (eofReader) Read([]byte) (int, error) {
	return 0, io.EOF
}
func (eofReader) ReadAt([]byte, int64) (int, error) {
	return 0, io.EOF
}
func (eofReader) Size() int64 {
	return 0
}

type MIOReader interface {
	Read(p []byte) (n int, err error)
	ReadAt(p []byte, offset int64) (n int, err error)
	Size() int64
}

type multiIOReader struct {
	readers []MIOReader
}

func (mr *multiIOReader) Size() (n int64) {
	for _, reader := range mr.readers {
		n += reader.Size()
	}
	return n
}

func (mr *multiIOReader) ReadAt(p []byte, offset int64) (n int, err error) {
	for len(mr.readers) > 0 {
		// Optimization to flatten nested multiIOReaders (Issue 13558).
		if len(mr.readers) == 1 {
			if r, ok := mr.readers[0].(*multiIOReader); ok {
				mr.readers = r.readers
				continue
			}
		}
		if mr.readers[0].Size() <= offset {
			offset = offset - mr.readers[0].Size()
			mr.readers[0] = eofReader{} // permit earlier GC
			mr.readers = mr.readers[1:]
			continue
		}

		n, err = mr.readers[0].ReadAt(p, offset)
		if err == io.EOF {
			// Use eofReader instead of nil to avoid nil panic
			mr.readers[0] = eofReader{} // permit earlier GC
			mr.readers = mr.readers[1:]
		}
		if n > 0 || err != io.EOF {
			if err == io.EOF && len(mr.readers) > 0 {
				// Don't return EOF yet. More readers remain.
				err = nil
			}
			return
		}
	}
	return 0, io.EOF
}

func (mr *multiIOReader) Read(p []byte) (n int, err error) {
	for len(mr.readers) > 0 {
		// Optimization to flatten nested multiIOReaders (Issue 13558).
		if len(mr.readers) == 1 {
			if r, ok := mr.readers[0].(*multiIOReader); ok {
				mr.readers = r.readers
				continue
			}
		}
		n, err = mr.readers[0].Read(p)
		if err == io.EOF {
			// Use eofReader instead of nil to avoid nil panic
			// after performing flatten (Issue 18232).
			mr.readers[0] = eofReader{} // permit earlier GC
			mr.readers = mr.readers[1:]
		}
		if n > 0 || err != io.EOF {
			if err == io.EOF && len(mr.readers) > 0 {
				// Don't return EOF yet. More readers remain.
				err = nil
			}
			return
		}
	}
	return 0, io.EOF
}

func (mr *multiIOReader) WriteTo(w io.Writer) (sum int64, err error) {
	return mr.writeToWithBuffer(w, make([]byte, 1024*32))
}

func (mr *multiIOReader) writeToWithBuffer(w io.Writer, buf []byte) (sum int64, err error) {
	for i, r := range mr.readers {
		var n int64
		if subMr, ok := r.(*multiIOReader); ok { // reuse buffer with nested multiIOReaders
			n, err = subMr.writeToWithBuffer(w, buf)
		} else {
			n, err = io.CopyBuffer(w, r, buf)
		}
		sum += n
		if err != nil {
			mr.readers = mr.readers[i:] // permit resume / retry after error
			return sum, err
		}
		mr.readers[i] = nil // permit early GC
	}
	mr.readers = nil
	return sum, nil
}

var _ io.WriterTo = (*multiIOReader)(nil)

// MultiIOReader returns a Reader that's the logical concatenation of
// the provided input readers. They're read sequentially. Once all
// inputs have returned EOF, Read will return EOF.  If any of the readers
// return a non-nil, non-EOF error, Read will return that error.
func MultiIOReader(readers ...MIOReader) MIOReader {
	r := make([]MIOReader, len(readers))
	copy(r, readers)
	return &multiIOReader{r}
}
