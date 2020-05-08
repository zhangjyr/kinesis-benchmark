package generator

import (
	"math/rand"
	"time"
)

var (
	BlobSize = 64 * 1000  // 64K
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type BlobGenerator struct {
}

func (g *BlobGenerator) Get() []byte {
	val := make([]byte, BlobSize)
	rand.Read(val)
	return val
}
