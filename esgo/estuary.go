//go:build linux && (amd64 || arm64 || riscv64)

//only work on 64-bit litte-endian machine

package esgo

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"

	"time"
	"unsafe"
)

func getSeed() uint64 {
	//return 1596176575357415943
	return uint64(time.Now().UnixNano())
}

type Estuary struct {
	lock          *sync.Mutex
	resource      []byte
	meta          *metaInfo
	table         []uint64
	data          []byte
	maxKeyLen     uint32
	maxValLen     uint32
	seed          uint64
	totalBlock    uint64
	spareBlock    uint64
	reservedBlock uint64
	sweeping      int32
	size          int
}

func (es *Estuary) Valid() bool {
	return es.meta != nil
}

func (es *Estuary) MaxKeyLen() uint32 {
	return es.maxKeyLen
}
func (es *Estuary) MaxValLen() uint32 {
	return es.maxValLen
}

func (es *Estuary) Item() uint64 {
	if es.meta == nil {
		return 0
	}
	return es.meta.item
}

func (es *Estuary) ItemLimit() uint64 {
	if es.meta == nil {
		return 0
	}
	return calcItemLimit(es.meta.totalEntry)
}

func (es *Estuary) DataFree() uint64 {
	if es.meta == nil {
		return 0
	}
	return (es.meta.freeBlock - es.spareBlock) * BlockSize
}

const MAGIC uint32 = 0xE9998888

type metaInfo struct {
	magic       uint32
	kvLimit     uint32
	seed        uint64
	item        uint64
	totalEntry  uint64
	cleanEntry  uint64
	totalBlock  uint64
	freeBlock   uint64
	blockCursor uint64
}

func cutTag(code uint64) uint64 {
	return code >> 56
}

func getTag(entry uint64) uint64 {
	return entry >> 56
}

func setTag(entry, tag uint64) uint64 {
	return (entry & ((uint64(1) << 56) - 1)) | (tag << 56)
}

func getBlk(entry uint64) uint64 {
	return entry & ((uint64(1) << 39) - 1)
}

func setBlk(entry, blk uint64) uint64 {
	return (entry & ^((uint64(1) << 39) - 1)) | (blk & ((uint64(1) << 39) - 1))
}

func testFit(entry uint64) bool {
	return (entry & (uint64(1) << 39)) != 0
}

func setFit(entry uint64) uint64 {
	return entry | (uint64(1) << 39)
}

func clearFit(entry uint64) uint64 {
	return entry & ^(uint64(1) << 39)
}

func getSft(entry uint64) uint64 {
	return (entry >> 40) & uint64(0xf)
}

func setSft(entry, sft uint64) uint64 {
	return (entry & ^(uint64(0xf) << 40)) | ((sft & 0xf) << 40)
}

func getTip(entry uint64) uint64 {
	return (entry >> 44) & uint64(0xfff)
}

func setTip(entry, tip uint64) uint64 {
	return (entry & ^(uint64(0xfff) << 44)) | ((tip & 0xfff) << 44)
}

// 39:1:4:12:8
func newEntry(blk, tip, tag, sft uint64) uint64 {
	if sft > MaxSft {
		sft = MaxSft
	}
	return (blk & ((uint64(1) << 39) - 1)) |
		(sft << 40) | ((tip & 0xfff) << 44) | (tag << 56)
}

func getKeyLen(mark uint32) uint32 {
	return mark & 0xff
}

func getValLen(mark uint32) uint32 {
	return mark >> 8
}

func calcPadding(keyLen, valLen int) uint64 {
	sz := uint64(keyLen+valLen) + 4
	return (sz+BlockSize-1) & ^(BlockSize-1) - sz
}

func calcBlock(keyLen, valLen uint32) uint64 {
	sz := uint64(keyLen+valLen) + 4
	return (sz + BlockSize - 1) / BlockSize
}

func calcBlockFromMark(mark uint32) uint64 {
	return calcBlock(getKeyLen(mark), getValLen(mark))
}

func markforRecord(klen, vlen int) uint32 {
	return (uint32(klen) & 0xff) | (uint32(vlen) << 8)
}

func markFormEmpty(bcnt uint64) uint64 {
	return bcnt << 8
}

func getBcnt(mark uint64) uint64 {
	return mark >> 8
}

func isFreeSection(mark uint64) bool {
	return (mark & 0xff) == 0
}

func clacSize(meta *metaInfo) uint64 {
	return uint64(unsafe.Sizeof(*meta)) + meta.totalEntry*8 + meta.totalBlock*BlockSize
}

const (
	MaxAddr      = ((uint64(1) << 39) - 1)
	ReservedAddr = ((uint64(1) << 39) - 2)
	BlockSize    = uint64(8)
	CleanEntry   = MaxAddr
	DeletedEntry = ReservedAddr

	DataReserveFactor  = uint64(10)
	EntryReserveFactor = uint64(8)
	MaxSft             = uint64(0xf)

	MinEntry = uint64(256)
	MaxEntry = uint64(1) << 34
)

func calcTotalEntry(item uint64) uint64 { return item * 3 / 2 }
func calcItemLimit(entry uint64) uint64 { return entry * 2 / 3 }

func isEmpty(entry uint64) bool {
	return getBlk(entry) >= ReservedAddr
}

func isClean(entry uint64) bool {
	return getBlk(entry) > ReservedAddr
}

func cast[T any](p *byte) *T {
	return (*T)(unsafe.Pointer(p))
}

func extractRecord(mark uint32, data []byte) (key, val []byte) {
	klen, vlen := getKeyLen(mark), getValLen(mark)
	return data[4 : 4+klen], data[4+klen : 4+klen+vlen]
}

func (es *Estuary) rMark(off uint64) *uint32 {
	return cast[uint32](&es.data[off])
}

func (es *Estuary) sMark(off uint64) *uint64 {
	return cast[uint64](&es.data[off])
}

func (es *Estuary) fetch(code uint64, key []byte) ([]byte, bool) {
	pos := code % uint64(len(es.table))
	tag := cutTag(code)
	for i := 0; i < len(es.table); i++ {
		e := atomic.LoadUint64(&es.table[pos])
	retry:
		if isEmpty(e) {
			if isClean(e) {
				return nil, false
			}
		} else if getTag(e) == tag {
			off := getBlk(e) * BlockSize
			mark := atomic.LoadUint32(es.rMark(off))
			t := atomic.LoadUint64(&es.table[pos])
			if e != t {
				e = t
				goto retry
			}
			rKey, rVal := extractRecord(mark, es.data[off:])
			if bytes.Equal(key, rKey) {
				val := make([]byte, len(rVal))
				copy(val, rVal)
				t = atomic.LoadUint64(&es.table[pos])
				if e != t {
					e = t
					goto retry
				}
				return val, true
			}
		}
		pos++
		if pos >= uint64(len(es.table)) {
			pos = 0
		}
	}
	return nil, false
}

func (es *Estuary) Fetch(key []byte) ([]byte, bool) {
	if es.meta == nil {
		return nil, false
	}
	code := hash(es.seed, key)
	val, got := es.fetch(code, key)
	if !got && es.sweeping != 0 {
		val, got = es.fetch(code, key)
		if !got {
			val, got = es.fetch(code, key)
		}
	}
	return val, got
}

func (es *Estuary) erase(key []byte) bool {
	code := hash(es.seed, key)
	pos := code % uint64(len(es.table))
	tag := cutTag(code)
	for i := 0; i < len(es.table); i++ {
		e := es.table[pos]
		if isEmpty(e) {
			if isClean(e) {
				return false
			}
		} else if getTag(e) == tag {
			off := getBlk(e) * BlockSize
			mark := *es.rMark(off)
			rKey, _ := extractRecord(mark, es.data[off:])
			if bytes.Equal(key, rKey) {
				atomic.StoreUint64(&es.table[pos], DeletedEntry)
				es.meta.item--
				bcnt := calcBlockFromMark(mark)
				*es.sMark(off) = markFormEmpty(bcnt)
				es.meta.freeBlock += bcnt
				return true
			}
		}
		pos++
		if pos >= uint64(len(es.table)) {
			pos = 0
		}
	}
	return false
}

func (es *Estuary) Erase(key []byte) bool {
	if es.meta == nil || len(key) == 0 || len(key) > int(es.maxKeyLen) {
		return false
	}
	es.lock.Lock()
	defer es.lock.Unlock()
	return es.erase(key)
}

func (es *Estuary) Update(key, val []byte) bool {
	if es.meta == nil || len(key) == 0 || len(key) > int(es.maxKeyLen) ||
		len(val) > int(es.maxValLen) {
		return false
	}
	es.lock.Lock()
	defer es.lock.Unlock()
	return es.update(key, val)
}

var debug = false

func (es *Estuary) update(key, val []byte) bool {
	newBcnt := calcBlock(uint32(len(key)), uint32(len(val)))
	if es.meta.freeBlock < newBcnt+es.spareBlock ||
		calcTotalEntry(es.meta.item) > uint64(len(es.table)) {
		return false
	}

	if es.meta.cleanEntry <= uint64(len(es.table))/EntryReserveFactor {
		//x times random input brings 1-1/e^x coverage，x = ln(ENTRY_RESERVE_FACTOR)
		//this procedure is slow, but rarely happen

		atomic.StoreInt32(&es.sweeping, -1)
		if es.sweep(false) {
			es.sweep(true)
		}

		item, dirty := uint64(0), uint64(0)
		for i := 0; i < len(es.table); i++ {
			if isEmpty(es.table[i]) {
				if testFit(es.table[i]) {
					dirty++
					es.table[i] = clearFit(es.table[i])
				} else {
					es.table[i] = CleanEntry
				}
			} else {
				item++
				es.table[i] = clearFit(es.table[i])
			}
		}

		atomic.StoreInt32(&es.sweeping, 0)

		es.meta.cleanEntry = uint64(len(es.table)) - item - dirty
	}

	code := hash(es.seed, key)
	origin := CleanEntry

	for {
		cur := es.meta.blockCursor * BlockSize
		bcnt := getBcnt(*es.sMark(cur))
		if bcnt >= newBcnt+es.reservedBlock {
			break
		}
		next := es.meta.blockCursor + bcnt
		if next == es.totalBlock {
			vic := uint64(0)
			for vic < cur {
				off := vic * BlockSize
				if isFreeSection(*es.sMark(off)) {
					vic += getBcnt(*es.sMark(off))
				} else if vic < newBcnt+es.reservedBlock {
					bcnt = calcBlockFromMark(*es.rMark(off))
					if getBcnt(*es.sMark(cur)) < bcnt {
						break
					}
					es.moveRecord(code, key, vic, &origin)
					vic += bcnt
					if es.meta.blockCursor == es.totalBlock {
						break
					}
				} else {
					break
				}
			}
			*es.sMark(0) = markFormEmpty(vic)
			es.meta.blockCursor = 0
		} else {
			off := next * BlockSize
			if isFreeSection(*es.sMark(off)) {
				bcnt = getBcnt(*es.sMark(off))
			} else {
				bcnt = calcBlockFromMark(*es.rMark(off))
				es.moveRecord(code, key, next, &origin)
				cur = es.meta.blockCursor * BlockSize
			}
			bcnt += getBcnt(*es.sMark(cur))
			*es.sMark(cur) = markFormEmpty(bcnt)
		}
	}

	es.meta.freeBlock -= newBcnt
	off := es.meta.blockCursor * BlockSize
	neo := es.meta.blockCursor
	es.meta.blockCursor += newBcnt
	cur := es.meta.blockCursor * BlockSize
	*es.sMark(cur) = markFormEmpty(getBcnt(*es.sMark(off)) - newBcnt)
	tip := fiilRecord(key, val, es.data[off:])

	pos := code % uint64(len(es.table))
	tag := cutTag(code)

	bookmark := struct {
		entry *uint64
		value uint64
	}{}
	for i := 0; i < len(es.table); i++ {
		e := es.table[pos]
		if isEmpty(e) {
			if bookmark.entry == nil {
				bookmark.entry = &es.table[pos]
				bookmark.value = newEntry(neo, tip, tag, uint64(i))
			}
			if isClean(e) {
				break
			}
		} else if getTag(e) == tag {
			xff := getBlk(e) * BlockSize
			mark := *es.rMark(xff)
			rKey, rVal := extractRecord(mark, es.data[xff:])
			if bytes.Equal(key, rKey) {
				bcnt := calcBlockFromMark(mark)
				if bytes.Equal(val, rVal) { //rollback
					es.meta.blockCursor = neo
					*es.sMark(off) = markFormEmpty(getBcnt(*es.sMark(cur)) + bcnt)
				} else {
					et := newEntry(neo, tip, tag, uint64(i))
					if et == origin {
						et = setTip(et, tip^1)
					}
					atomic.StoreUint64(&es.table[pos], et)
					*es.sMark(xff) = markFormEmpty(bcnt)
				}
				es.meta.freeBlock += bcnt
				return true
			}
		}
		pos++
		if pos >= uint64(len(es.table)) {
			pos = 0
		}
	}
	if bookmark.entry != nil {
		if isClean(*bookmark.entry) {
			es.meta.cleanEntry--
		}
		atomic.StoreUint64(bookmark.entry, bookmark.value)
		es.meta.item++
		return true
	}
	return false
}

func (es *Estuary) sweep(end bool) bool {
	moved := false
	for i := 0; i < len(es.table); i++ {
		if isEmpty(es.table[i]) || testFit(es.table[i]) {
			continue
		}
		pos := uint64(0)
		if sft := getSft(es.table[i]); sft < MaxSft {
			if i < int(sft) {
				pos = uint64(len(es.table)+i) - sft
			} else {
				pos = uint64(i) - sft
			}
		} else {
			off := getBlk(es.table[i]) * BlockSize
			mark := *es.rMark(off)
			rKey, _ := extractRecord(mark, es.data[off:])
			pos = hash(es.seed, rKey) % uint64(len(es.table))
		}
		fit := true
		for j := 0; j < len(es.table); j++ {
			if isEmpty(es.table[pos]) {
				moved = true
				sft := uint64(j)
				if sft > MaxSft {
					sft = MaxSft
				}
				es.table[pos] = setSft(es.table[i], sft)
				if fit {
					es.table[pos] = setFit(es.table[pos])
				}
				e := DeletedEntry
				if end {
					e = setFit(e)
				}
				atomic.StoreUint64(&es.table[i], e)
				break
			} else if !testFit(es.table[pos]) {
				if uint64(i) == pos {
					if fit {
						es.table[i] = setFit(es.table[i])
					}
					break
				}
				fit = false
			}
			pos++
			if pos >= uint64(len(es.table)) {
				pos = 0
			}
		}
	}
	return moved
}

func (es *Estuary) moveRecord(code uint64, key []byte, vic uint64, pent *uint64) {
	off := vic * BlockSize
	mark := *es.rMark(off)
	bcnt := calcBlockFromMark(mark)
	cur := es.meta.blockCursor * BlockSize
	size := bcnt * BlockSize
	copy(es.data[cur+8:cur+size], es.data[off+8:off+size])

	rKey, _ := extractRecord(mark, es.data[off:])
	rCode := hash(es.seed, rKey)
	if rCode != code || !bytes.Equal(key, rKey) {
		pent = nil
	}

	pos := rCode % uint64(len(es.table))
	for i := 0; i < len(es.table); i++ {
		e := es.table[pos]
		if isEmpty(e) {
			if isClean(e) {
				break
			}
		} else if getBlk(e) == vic {
			if pent != nil {
				*pent = e
			}
			next := es.meta.blockCursor + bcnt
			if next != es.totalBlock {
				*es.sMark(next * BlockSize) = markFormEmpty(getBcnt(*es.sMark(cur)) - bcnt)
			}
			*es.sMark(cur) = *es.sMark(off)
			e = setBlk(e, es.meta.blockCursor)
			atomic.StoreUint64(&es.table[pos], e)
			*es.sMark(off) = markFormEmpty(bcnt)
			es.meta.blockCursor = next
			return
		}
		pos++
		if pos >= uint64(len(es.table)) {
			pos = 0
		}
	}

	*es.sMark(off) = markFormEmpty(bcnt)
	es.meta.freeBlock += bcnt
}

func fiilRecord(key, val, dest []byte) uint64 {
	mark := markforRecord(len(key), len(val))
	*cast[uint32](&dest[0]) = mark
	ext := 4 + len(key)
	end := ext + len(val)
	copy(dest[4:ext], key)
	copy(dest[ext:end], val)
	return hash(uint64(mark), dest[4:end])
}

type Reader interface {
	io.Reader
	Size() int
}

func roundUp(n int) int {
	m := 0x1fffff
	return (n + m) & (^m)
}

func mapSegments(meta *metaInfo) (table []uint64, data []byte) {
	var tmp reflect.SliceHeader
	tmp.Data = uintptr(unsafe.Pointer(meta)) + unsafe.Sizeof(*meta)
	tmp.Len = int(meta.totalEntry)
	tmp.Cap = tmp.Len
	table = *(*[]uint64)(unsafe.Pointer(&tmp))

	tmp.Data += uintptr(meta.totalEntry * 8)
	tmp.Len = int(meta.totalBlock * BlockSize)
	tmp.Cap = tmp.Len
	data = *(*[]byte)(unsafe.Pointer(&tmp))

	return table, data
}

func (es *Estuary) Load(src Reader) error {
	if es.meta != nil {
		return errors.New("double init")
	}
	size := src.Size()
	if size <= int(unsafe.Sizeof(*es.meta)) {
		return errors.New("bad source")
	}
	res, err := syscall.Mmap(-1, 0, roundUp(size), syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS|syscall.MAP_HUGETLB)
	if err == syscall.ENOMEM {
		res, err = syscall.Mmap(-1, 0, roundUp(size), syscall.PROT_READ|syscall.PROT_WRITE,
			syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS)
	}
	if err != nil {
		return err
	}
	defer func() {
		if res != nil {
			syscall.Munmap(res)
		}
	}()

	for n := 0; n < size; {
		m, err := src.Read(res[n:])
		if err != nil {
			return err
		}
		n += m
	}

	meta := cast[metaInfo](&res[0])
	es.maxKeyLen = getKeyLen(meta.kvLimit)
	es.maxValLen = getValLen(meta.kvLimit)
	es.seed = meta.seed
	es.sweeping = 0
	es.totalBlock = meta.totalBlock
	es.reservedBlock = calcBlock(es.maxKeyLen, es.maxValLen) * 2
	if meta.magic != MAGIC ||
		meta.totalEntry < MinEntry || meta.totalEntry > MaxEntry ||
		meta.totalBlock <= es.reservedBlock || meta.totalBlock > ReservedAddr ||
		size < int(clacSize(meta)) {
		return errors.New("broken data")
	}
	es.spareBlock = es.reservedBlock + (es.totalBlock-es.reservedBlock)/DataReserveFactor

	es.table, es.data = mapSegments(meta)

	es.resource, res = res, nil
	es.meta = meta
	es.lock = new(sync.Mutex)
	es.size = size
	return nil
}

func (es *Estuary) Dump(out io.Writer) error {
	if es.meta == nil {
		return errors.New("uninitialized")
	}
	es.lock.Lock()
	defer es.lock.Unlock()
	for n := 0; n < es.size; {
		m, err := out.Write(es.resource[n:es.size])
		if err != nil {
			return err
		}
		n += m
	}
	return nil
}

func (es *Estuary) Destroy() {
	if res := es.resource; res != nil {
		syscall.Munmap(res)
	}
	*es = Estuary{}
}

type file struct {
	fd   int
	size int
}

func (rd *file) Read(buf []byte) (int, error) {
	return syscall.Read(rd.fd, buf)
}

func (rd *file) Write(buf []byte) (int, error) {
	return syscall.Write(rd.fd, buf)
}

func (rd *file) Size() int {
	return rd.size
}

func (es *Estuary) DumpFile(filename string) error {
	fd, err := syscall.Open(filename,
		syscall.O_CREAT|syscall.O_TRUNC|syscall.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer syscall.Close(fd)
	rd := &file{fd: fd}
	return es.Dump(rd)
}

func LoadFile(filename string) (*Estuary, error) {
	fd, err := syscall.Open(filename, syscall.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer syscall.Close(fd)

	st := &syscall.Stat_t{}
	if err = syscall.Fstat(fd, st); err != nil {
		return nil, err
	}
	rd := &file{fd: fd, size: int(st.Size)}

	es := &Estuary{}
	if err = es.Load(rd); err != nil {
		return nil, err
	}
	return es, nil
}

type Source interface {
	Get() (key, val []byte)
	Total() int
	Reset()
}

type Config struct {
	ItemLimit   uint64
	MaxKeyLen   uint32
	MaxValLen   uint32
	AvgItemSize uint32
}

var errOutOfCapacity = errors.New("out of capacity")

func create(filename string, cfg *Config, totalBlock uint64, src Source) (uint64, error) {
	header := metaInfo{
		magic:       MAGIC,
		kvLimit:     markforRecord(int(cfg.MaxKeyLen), int(cfg.MaxValLen)),
		seed:        getSeed(),
		item:        0,
		blockCursor: 0,
		totalEntry:  calcTotalEntry(cfg.ItemLimit),
	}
	header.totalBlock = (uint64(cfg.AvgItemSize+4) + BlockSize/2) *
		(cfg.ItemLimit + 1) / BlockSize
	initEnd := header.totalBlock
	header.totalBlock += header.totalBlock/(DataReserveFactor-1) + 1
	header.totalBlock += calcBlock(cfg.MaxKeyLen, cfg.MaxValLen) * 2
	if header.totalBlock > ReservedAddr {
		return 0, errors.New("too big")
	}
	header.cleanEntry = header.totalEntry
	header.freeBlock = header.totalBlock

	size := int(clacSize(&header))

	fd, err := syscall.Open(filename,
		syscall.O_CREAT|syscall.O_TRUNC|syscall.O_RDWR, 0644)
	if err != nil {
		return 0, err
	}
	defer syscall.Close(fd)

	if err = syscall.Ftruncate(fd, int64(size)); err != nil {
		return 0, err
	}
	space, err := syscall.Mmap(fd, 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return 0, err
	}
	defer syscall.Munmap(space)

	meta := cast[metaInfo](&space[0])
	*meta = header
	table, data := mapSegments(meta)
	for i := 0; i < len(table); i++ {
		table[i] = CleanEntry
	}

	total := 0
	if src != nil {
		total = src.Total()
		if total < 0 || total > int(cfg.ItemLimit) {
			return 0, errors.New("bad source")
		}
	}
	paddingSum := uint64(0)

	for i := 0; i < total; i++ {
		key, val := src.Get()
		if len(key) == 0 || len(key) > int(cfg.MaxKeyLen) || len(val) > int(cfg.MaxValLen) {
			return 0, errors.New("bad source")
		}
		code := hash(meta.seed, key)
		tag := cutTag(code)
		pos := code % uint64(len(table))
		for j := 0; j < len(table); j++ {
			if isEmpty(table[pos]) {
				meta.item++
				meta.cleanEntry--
				goto addOne
			} else if getTag(table[pos]) == tag {
				off := getBlk(table[pos]) * BlockSize
				mark := *cast[uint32](&data[off])
				rKey, _ := extractRecord(mark, data[off:])
				if bytes.Equal(key, rKey) {
					bcnt := calcBlockFromMark(mark)
					*cast[uint64](&data[off]) = markFormEmpty(bcnt)
					meta.freeBlock += bcnt
					goto addOne
				}
			}
			pos++
			if pos >= uint64(len(table)) {
				pos = 0
			}
			continue
		addOne:
			bcnt := calcBlock(uint32(len(key)), uint32(len(val)))
			paddingSum += calcPadding(len(key), len(val))
			off := meta.blockCursor * BlockSize
			neo := meta.blockCursor
			meta.blockCursor += bcnt
			if meta.blockCursor > initEnd {
				return paddingSum/uint64(i+1) + 1, errOutOfCapacity
			}
			meta.freeBlock -= bcnt
			tip := fiilRecord(key, val, data[off:])
			table[pos] = newEntry(neo, tip, tag, uint64(j))
			break
		}
	}

	off := meta.blockCursor * BlockSize
	*cast[uint64](&data[off]) = markFormEmpty(meta.totalBlock - meta.blockCursor)
	return 0, nil
}

func Create(filename string, cfg *Config, src Source) error {
	if calcTotalEntry(cfg.ItemLimit) < MinEntry || calcTotalEntry(cfg.ItemLimit) > MaxEntry ||
		cfg.MaxKeyLen == 0 || cfg.MaxKeyLen >= (uint32(1)<<8) ||
		cfg.MaxValLen == 0 || cfg.MaxValLen >= (uint32(1)<<24) ||
		cfg.AvgItemSize < 2 || cfg.AvgItemSize > cfg.MaxKeyLen+cfg.MaxValLen {
		return errors.New("illegal config")
	}

	avgItemSize := uint64(cfg.AvgItemSize + 4)
	totalBlock := (avgItemSize + BlockSize/2) * (cfg.ItemLimit + 1) / BlockSize
	padding, err := create(filename, cfg, totalBlock, src)
	if err == errOutOfCapacity && padding > BlockSize/2 {
		totalBlock = (avgItemSize + padding) * (cfg.ItemLimit + 1) / BlockSize
		_, err = create(filename, cfg, totalBlock, src)
	}
	return err
}

func Extend(filename string, percent int, cfg *Config) error {
	if percent <= 0 || percent > 1000 {
		return errors.New("illegal parameters")
	}
	fd, err := syscall.Open(filename, syscall.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer syscall.Close(fd)

	st := &syscall.Stat_t{}
	if err = syscall.Fstat(fd, st); err != nil {
		return err
	}
	size := int(st.Size)

	var meta *metaInfo
	temp := make([]byte, unsafe.Sizeof(*meta))
	if _, err = syscall.Read(fd, temp); err != nil {
		return err
	}
	meta = cast[metaInfo](&temp[0])

	maxKeyLen := getKeyLen(meta.kvLimit)
	maxValLen := getValLen(meta.kvLimit)
	reservedBlock := calcBlock(maxKeyLen, maxValLen) * 2
	bcnt := meta.totalBlock - reservedBlock
	extBcnt := (bcnt*uint64(percent) + 99) / 100
	if meta.magic != MAGIC ||
		meta.totalEntry < MinEntry || meta.totalEntry > MaxEntry ||
		meta.totalBlock <= reservedBlock || meta.totalBlock+extBcnt > ReservedAddr ||
		size < int(clacSize(meta)) {
		return errors.New("broken data")
	}

	size += int(extBcnt * BlockSize)
	if err = syscall.Ftruncate(fd, int64(size)); err != nil {
		return err
	}
	space, err := syscall.Mmap(fd, 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	defer syscall.Munmap(space)

	*cast[uint64](&space[st.Size]) = markFormEmpty(extBcnt)
	meta = cast[metaInfo](&space[0])
	meta.totalBlock += extBcnt
	meta.freeBlock += extBcnt

	if cfg != nil {
		cfg.MaxKeyLen = maxKeyLen
		cfg.MaxValLen = maxValLen
		cfg.ItemLimit = calcItemLimit(meta.totalEntry)
		bcnt += extBcnt
		bcnt -= bcnt / DataReserveFactor
		cfg.AvgItemSize = uint32((bcnt*BlockSize-cfg.ItemLimit*(BlockSize/2))/cfg.ItemLimit) - 4
	}
	return nil
}
