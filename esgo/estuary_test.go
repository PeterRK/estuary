//go:build linux && (amd64 || arm64 || riscv64)

package esgo

import (
	"bytes"
	"encoding/binary"
	"testing"
)

const tPiece = 1000

var tCfg = &Config{
	ItemLimit:   uint64(tPiece),
	MaxKeyLen:   8,
	MaxValLen:   255,
	AvgItemSize: 255/2 + 1 + 8,
}

type generator struct {
	curr  uint64
	begin uint64
	total uint64
	shift uint8
	val   [255]byte
	key   [8]byte
}

func (g *generator) init(begin, total uint64, shift uint8) {
	g.curr = begin - 1
	g.begin = begin
	g.total = total
	g.shift = shift
}

func (g *generator) Reset() {
	g.curr = g.begin - 1
}

func (g *generator) Total() int {
	return int(g.total)
}

func (g *generator) Get() (key, val []byte) {
	g.curr++
	l := uint8(g.curr) + g.shift
	for i := uint8(0); i < l; i++ {
		g.val[i] = byte(l)
	}
	binary.LittleEndian.PutUint64(g.key[:], g.curr)
	return g.key[:], g.val[:l]
}

func assert(t *testing.T, state bool) {
	if !state {
		t.FailNow()
	}
}

func TestBuildAndRead(t *testing.T) {
	const filename = "tmp.es"

	var src = &generator{}
	src.init(0, tPiece, 5)
	assert(t, Create(filename, tCfg, src) == nil)

	dict, err := LoadFile(filename)
	assert(t, err == nil && dict.Valid())
	defer dict.Release()
	assert(t, dict.MaxKeyLen() == tCfg.MaxKeyLen)
	assert(t, dict.MaxValLen() == tCfg.MaxValLen)
	assert(t, dict.Item() == tPiece)

	src.Reset()
	for i := 0; i < tPiece; i++ {
		key, val := src.Get()
		rVal, got := dict.Fetch(key)
		assert(t, got)
		assert(t, bytes.Equal(val, rVal))
	}
	key := [8]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	_, got := dict.Fetch(key[:])
	assert(t, !got)
}

func TestUpdate(t *testing.T) {
	const filename = "update.es"

	var src1, src2 = &generator{}, &generator{}
	src1.init(0, tPiece, 5)
	assert(t, Create(filename, tCfg, src1) == nil)

	cfg := &Config{}
	assert(t, Extend(filename, 1, cfg) == nil)
	assert(t, cfg.ItemLimit == tCfg.ItemLimit)
	assert(t, cfg.AvgItemSize > tCfg.AvgItemSize)

	dict, err := LoadFile(filename)
	assert(t, err == nil && dict.Valid())
	defer dict.Release()

	src1.Reset()
	for i := 0; i < tPiece; i++ {
		key, _ := src1.Get()
		if i%2 != 0 {
			assert(t, dict.Erase(key))
		}
	}

	src2.init(1, tPiece, 10)
	for i := 1; i < tPiece; i++ {
		key, val := src2.Get()
		assert(t, dict.Update(key, val))
	}

	src1.Reset()
	key, val := src1.Get()
	rVal, got := dict.Fetch(key)
	assert(t, got)
	assert(t, bytes.Equal(val, rVal))

	src2.Reset()
	for i := 1; i < tPiece; i++ {
		key, val = src2.Get()
		rVal, got := dict.Fetch(key)
		assert(t, got)
		assert(t, bytes.Equal(val, rVal))

		eKey, eVal := src1.Get()
		assert(t, bytes.Equal(key, eKey) && len(eVal) != len(rVal))
		assert(t, dict.Update(eKey, eVal))
	}

	src1.Reset()
	for i := 0; i < tPiece; i++ {
		key, val := src1.Get()
		rVal, got := dict.Fetch(key)
		assert(t, got)
		assert(t, bytes.Equal(val, rVal))
	}
}

func TestErase(t *testing.T) {
	const filename = "erase.es"
	assert(t, Create(filename, tCfg, nil) == nil)

	dict, err := LoadFile(filename)
	assert(t, err == nil && dict.Valid())
	defer dict.Release()

	var src1, src2 = &generator{}, &generator{}
	src1.init(0, tPiece*4, 5)
	src2.init(0, tPiece*3, 10)

	for k := 0; k < 3; k++ {
		for i := 0; i < tPiece; i++ {
			key, val := src1.Get()
			assert(t, dict.Update(key, val))
		}
		for i := 0; i < tPiece; i++ {
			key, _ := src2.Get()
			assert(t, dict.Erase(key))
		}
	}

	for i := 0; i < tPiece; i++ {
		key, val := src1.Get()
		assert(t, dict.Update(key, val))
	}

	src2.Reset()
	for i := 0; i < tPiece*3; i++ {
		key, _ := src2.Get()
		_, got := dict.Fetch(key)
		assert(t, !got)
	}

	src1.init(tPiece*3, tPiece*4, 5)
	for i := 0; i < tPiece; i++ {
		key, val := src1.Get()
		rVal, got := dict.Fetch(key)
		assert(t, got)
		assert(t, bytes.Equal(val, rVal))
	}

	src1.Reset()
	src2.Reset()
	for i := 0; i < tPiece/2; i++ {
		key, _ := src1.Get()
		assert(t, dict.Erase(key))
	}
	for i := 0; i < tPiece/2; i++ {
		key, val := src2.Get()
		assert(t, dict.Update(key, val))
	}
	for i := tPiece / 2; i < tPiece; i++ {
		key, _ := src1.Get()
		assert(t, dict.Erase(key))
	}
	src1.init(0, tPiece, 11)
	for i := 0; i < tPiece/2; i++ {
		key, val := src1.Get()
		assert(t, dict.Update(key, val))
	}
	for i := tPiece / 2; i < tPiece; i++ {
		key, val := src2.Get()
		assert(t, dict.Update(key, val))
	}
	src1.Reset()
	src2.Reset()
	for i := tPiece / 2; i < tPiece; i++ {
		key, val := src1.Get()
		rVal, got := dict.Fetch(key)
		assert(t, got)
		assert(t, bytes.Equal(val, rVal))
		assert(t, dict.Erase(key))
	}
	for i := tPiece / 2; i < tPiece; i++ {
		key, _ := src2.Get()
		_, got := dict.Fetch(key)
		assert(t, !got)
	}
	for i := tPiece / 2; i < tPiece; i++ {
		key, val := src2.Get()
		rVal, got := dict.Fetch(key)
		assert(t, got)
		assert(t, bytes.Equal(val, rVal))
	}
}
