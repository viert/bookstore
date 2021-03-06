package storage

import (
	"bytes"
	"fmt"
	"testing"
)

var (
	veryShortData = []byte("hello world")
	shortData     = []byte(` Seeker is the interface that wraps the basic Seek method.
Seek sets the offset for the next Read or Write to offset, interpreted according to whence: SeekStart means relative to the start of the file, SeekCurrent means relative to the current offset, and SeekEnd means relative to the end. Seek returns the new offset relative to the start of the file and an error, if any.
Seeking to an offset before the start of the file is an error. Seeking to any positive offset is legal, but the behavior of subsequent I/O operations on the underlying object is implementation-dependent. `)
	longData = []byte(` Reader is the interface that wraps the basic Read method.
Read reads up to len(p) bytes into p. It returns the number of bytes read (0 <= n <= len(p)) and any error encountered. Even if Read returns n < len(p), it may use all of p as scratch space during the call. If some data is available but not len(p) bytes, Read conventionally returns what is available instead of waiting for more.
When Read encounters an error or end-of-file condition after successfully reading n > 0 bytes, it returns the number of bytes read. It may return the (non-nil) error from the same call or return the error (and n == 0) from a subsequent call. An instance of this general case is that a Reader returning a non-zero number of bytes at the end of the input stream may return either err == EOF or err == nil. The next Read should return 0, EOF.
Callers should always process the n > 0 bytes returned before considering the error err. Doing so correctly handles I/O errors that happen after reading some bytes and also both of the allowed EOF behaviors.
Implementations of Read are discouraged from returning a zero byte count with a nil error, except when len(p) == 0. Callers should treat a return of 0 and nil as indicating that nothing happened; in particular it does not indicate EOF.
Implementations must not retain p. `)
)

func TestBackend(t *testing.T) {
	mb := NewMemBackend()
	CreateStorage(mb, 512, 512, 0)
	expectedLen := storeHeaderSize + 512*(chunkHeaderSize+512)
	if len(mb.data) != expectedLen {
		t.Errorf("data len is expected to be %d, got %d instead", expectedLen, len(mb.data))
	}
}

func replicationSucceeded(idx int) error {
	return nil
}

func replicationFailed(idx int) error {
	return fmt.Errorf("replication error")
}

func TestStore(t *testing.T) {
	mb := NewMemBackend()
	CreateStorage(mb, 512, 512, 0)
	st, err := Open(mb)
	if err != nil {
		t.Error(err)
	}

	// writing short data
	i, err := st.Write(shortData, replicationSucceeded)
	if err != nil {
		t.Error(err)
	}

	if i != 0 {
		t.Errorf("write idx is expected to be 0, got %d instead", i)
	}

	if st.header.FreeChunkIdx != 1 {
		t.Errorf("next free idx is expected to be 1, got %d instead", st.header.FreeChunkIdx)
	}

	// writing long data
	j, err := st.Write(longData, replicationSucceeded)
	if err != nil {
		t.Error(err)
	}

	if j != 1 {
		t.Errorf("write idx is expected to be 1, got %d instead", j)
	}

	if st.header.FreeChunkIdx != 3 {
		t.Errorf("next free idx is expected to be 3, got %d instead", st.header.FreeChunkIdx)
	}

	// reading short data
	data, err := st.Read(i)
	if err != nil {
		t.Error(err)
	}
	if string(data) != string(shortData) {
		t.Error("stored and recovered data don't match")
	}

	// reading long data
	data, err = st.Read(j)
	if err != nil {
		t.Error(err)
	}
	if string(data) != string(longData) {
		t.Error("stored and recovered data don't match")
	}

	// reading out of bounds

	_, err = st.Read(j + 2)
	if err == nil {
		t.Errorf("reading at position %d should cause an error", j+2)
	}
}

func TestReplication(t *testing.T) {
	replicationCalled := false

	mb := NewMemBackend()
	CreateStorage(mb, 512, 512, 104)
	st, err := Open(mb)
	if err != nil {
		t.Error(err)
	}

	// writing short data
	i, err := st.Write(shortData, func(idx int) error {
		replicationCalled = true
		if idx != 0 {
			t.Errorf("index of replicated item must be 0, got %d instead", idx)
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}

	if !replicationCalled {
		t.Errorf("replication callback wasn't called")
	}

	if i != 0 {
		t.Errorf("write idx is expected to be 0, got %d instead", i)
	}

	if st.header.FreeChunkIdx != 1 {
		t.Errorf("next free idx is expected to be 1, got %d instead", st.header.FreeChunkIdx)
	}

	replicationCalled = false
	// writing short data
	_, err = st.Write(longData, func(idx int) error {
		replicationCalled = true
		if idx != 1 {
			t.Errorf("index of replicated item must be 1, got %d instead", idx)
		}
		return fmt.Errorf("replication failed")
	})

	if err == nil {
		t.Error(err)
	}
}

func TestUncompressed(t *testing.T) {
	mb := NewMemBackend()
	CreateStorage(mb, 512, 512, 104)
	st, err := Open(mb)
	if err != nil {
		t.Error(err)
	}

	// writing very short data
	_, err = st.Write(veryShortData, replicationSucceeded)
	if err != nil {
		t.Error(err)
	}

	buf, count, gzipped, err := st.readRaw(0)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Errorf("size of data in chunks must be 1, got %d instead", count)
	}

	if gzipped {
		t.Errorf("gzipped should be false for very short data")
	}

	if !bytes.Equal(buf.Bytes(), veryShortData) {
		t.Errorf("raw data should be equal to input data. input is %v, read is %v", veryShortData, buf.Bytes())
	}

}
