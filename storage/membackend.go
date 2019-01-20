package storage

// MemBackend represents an in-memory backend for storage
// mostly for testing purposes
type MemBackend struct {
	data []byte
	idx  int
}

func NewMemBackend() *MemBackend {
	mb := new(MemBackend)
	mb.data = make([]byte, 0, 65536)
	mb.idx = 0
	return mb
}

func (mb *MemBackend) WriteAt(p []byte, off int64) (int, error) {
	off32 := int(off) // sure it won't overflow
	appendLen := off32 + len(p) - len(mb.data)
	if appendLen > 0 {
		mb.data = append(mb.data, make([]byte, appendLen)...)
	}
	copy(mb.data[off32:off32+len(p)], p)
	return len(p), nil
}

func (mb *MemBackend) ReadAt(p []byte, off int64) (int, error) {
	off32 := int(off) // sure it won't overflow
	readLen := len(mb.data) - off32
	if len(p) > readLen {
		readLen = len(p)
	}
	copy(p, mb.data[off32:off32+readLen])
	return readLen, nil
}

func (mb *MemBackend) Write(p []byte) (n int, err error) {
	n, err = mb.WriteAt(p, int64(mb.idx))
	if err != nil {
		return
	}
	mb.idx += n
	return
}
