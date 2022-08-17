package esgo

import (
	"encoding/binary"
)

func rot(x uint64, k int) uint64 {
	return (x << k) | (x >> (64 - k))
}

type state struct {
	a, b, c, d uint64
}

func (s *state) mix() {
	s.c = rot(s.c, 50)
	s.c += s.d
	s.a ^= s.c
	s.d = rot(s.d, 52)
	s.d += s.a
	s.b ^= s.d
	s.a = rot(s.a, 30)
	s.a += s.b
	s.c ^= s.a
	s.b = rot(s.b, 41)
	s.b += s.c
	s.d ^= s.b
	s.c = rot(s.c, 54)
	s.c += s.d
	s.a ^= s.c
	s.d = rot(s.d, 48)
	s.d += s.a
	s.b ^= s.d
	s.a = rot(s.a, 38)
	s.a += s.b
	s.c ^= s.a
	s.b = rot(s.b, 37)
	s.b += s.c
	s.d ^= s.b
	s.c = rot(s.c, 62)
	s.c += s.d
	s.a ^= s.c
	s.d = rot(s.d, 34)
	s.d += s.a
	s.b ^= s.d
	s.a = rot(s.a, 5)
	s.a += s.b
	s.c ^= s.a
	s.b = rot(s.b, 36)
	s.b += s.c
	s.d ^= s.b
}

func (s *state) end() {
	s.d ^= s.c
	s.c = rot(s.c, 15)
	s.d += s.c
	s.a ^= s.d
	s.d = rot(s.d, 52)
	s.a += s.d
	s.b ^= s.a
	s.a = rot(s.a, 26)
	s.b += s.a
	s.c ^= s.b
	s.b = rot(s.b, 51)
	s.c += s.b
	s.d ^= s.c
	s.c = rot(s.c, 28)
	s.d += s.c
	s.a ^= s.d
	s.d = rot(s.d, 9)
	s.a += s.d
	s.b ^= s.a
	s.a = rot(s.a, 47)
	s.b += s.a
	s.c ^= s.b
	s.b = rot(s.b, 54)
	s.c += s.b
	s.d ^= s.c
	s.c = rot(s.c, 32)
	s.d += s.c
	s.a ^= s.d
	s.d = rot(s.d, 25)
	s.a += s.d
	s.b ^= s.a
	s.a = rot(s.a, 63)
	s.b += s.a
}

func hash(seed uint64, key []byte) uint64 {
	const magic uint64 = 0xdeadbeefdeadbeef
	s := state{seed, seed, magic, magic}
	length := uint64(len(key))

	for ; len(key) >= 32; key = key[32:] {
		s.c += binary.LittleEndian.Uint64(key)
		s.d += binary.LittleEndian.Uint64(key[8:])
		s.mix()
		s.a += binary.LittleEndian.Uint64(key[16:])
		s.b += binary.LittleEndian.Uint64(key[24:])
	}
	if len(key) >= 16 {
		s.c += binary.LittleEndian.Uint64(key)
		s.d += binary.LittleEndian.Uint64(key[8:])
		s.mix()
		key = key[16:]
	}

	s.d += length << 56
	switch len(key) {
	case 15:
		s.d += (uint64(key[14]) << 48) |
			(uint64(binary.LittleEndian.Uint16(key[12:])) << 32) |
			uint64(binary.LittleEndian.Uint32(key[8:]))
		s.c += binary.LittleEndian.Uint64(key)
	case 14:
		s.d += (uint64(binary.LittleEndian.Uint16(key[12:])) << 32) |
			uint64(binary.LittleEndian.Uint32(key[8:]))
		s.c += binary.LittleEndian.Uint64(key)
	case 13:
		s.d += (uint64(key[12]) << 32) | uint64(binary.LittleEndian.Uint32(key[8:]))
		s.c += binary.LittleEndian.Uint64(key)
	case 12:
		s.d += uint64(binary.LittleEndian.Uint32(key[8:]))
		s.c += binary.LittleEndian.Uint64(key)
	case 11:
		s.d += (uint64(key[10]) << 16) | uint64(binary.LittleEndian.Uint16(key[8:]))
		s.c += binary.LittleEndian.Uint64(key)
	case 10:
		s.d += uint64(binary.LittleEndian.Uint16(key[8:]))
		s.c += binary.LittleEndian.Uint64(key)
	case 9:
		s.d += uint64(key[8])
		s.c += binary.LittleEndian.Uint64(key)
	case 8:
		s.c += binary.LittleEndian.Uint64(key)
	case 7:
		s.c += (uint64(key[6]) << 48) |
			(uint64(binary.LittleEndian.Uint16(key[4:])) << 32) |
			uint64(binary.LittleEndian.Uint32(key))
	case 6:
		s.c += (uint64(binary.LittleEndian.Uint16(key[4:])) << 32) |
			uint64(binary.LittleEndian.Uint32(key))
	case 5:
		s.c += (uint64(key[4]) << 32) | uint64(binary.LittleEndian.Uint32(key))
	case 4:
		s.c += uint64(binary.LittleEndian.Uint32(key))
	case 3:
		s.c += (uint64(key[2]) << 16) | uint64(binary.LittleEndian.Uint16(key))
	case 2:
		s.c += uint64(binary.LittleEndian.Uint16(key))
	case 1:
		s.c += uint64(key[0])
	case 0:
		s.c += magic
		s.d += magic

	}
	s.end()
	return s.a
}
