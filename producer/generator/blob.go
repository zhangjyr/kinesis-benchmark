package generator

import (
	"math/rand"
	"time"
)

const (
	BLOB_SIZE = 64 * 1000  // 64K
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type BlobGenerator struct {
}

func (g *BlobGenerator) Get() []byte {
	val := make([]byte, BLOB_SIZE)
	rand.Read(val)
	return val
}
