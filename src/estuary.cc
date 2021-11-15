//==============================================================================
// Dictionary designed for read-mostly scene.
// Copyright (C) 2020  Ruan Kunliang
//
// This library is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License as published by the Free
// Software Foundation; either version 2.1 of the License, or (at your option)
// any later version.
//
// This library is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
// details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the This Library; if not, see <https://www.gnu.org/licenses/>.
//==============================================================================

#include <cassert>
#include <pthread.h>
#include <estuary.h>
#include "internal.h"

namespace estuary {

const char* LockException::what() const noexcept {
	return "fail to handle lock";
}
const char* DataException::what() const noexcept {
	return "broken data";
}

static constexpr uint32_t MAGIC = 0xE998;
struct Estuary::Meta {
	uint16_t magic = MAGIC;
	uint8_t _pad = 0;
	bool writing = false;
	uint32_t kv_limit = 0;
	uint64_t seed = 0;
	size_t item = 0;
	size_t total_entry = 0;
	size_t clean_entry = 0;
	size_t total_block = 0;
	size_t free_block = 0;
	size_t block_cursor = 0;
};
using Header = Estuary::Meta;

size_t Estuary::item() const noexcept {
	return m_meta == nullptr? 0 : m_meta->item;
}

struct Estuary::Lock {
	pthread_mutex_t core;
	uint8_t _pad1[64U-(sizeof(pthread_mutex_t)&63U)];
	bool sweeping = false;
	uint8_t _pad2[7];
};

static constexpr size_t MAX_OFF_MARK = 15U;
static constexpr unsigned ADDR_BITWIDTH = 39U;		//4TB
static constexpr uint64_t MAX_ADDR = (1ULL << ADDR_BITWIDTH) - 1U;
static constexpr uint64_t RESERVED_ADDR = (1ULL << ADDR_BITWIDTH) - 2U;

static inline uint32_t CutTag(uint64_t code) {
	return code >> 56U;
}

struct Entry {
	uint64_t blk : ADDR_BITWIDTH;
	uint64_t fit : 1;
	uint64_t off : 4;
	uint64_t tip : 12;  //for handling ABA problem
	uint64_t tag : 8;
	explicit constexpr Entry(uint64_t blk_, uint64_t tip_=0, uint64_t tag_=0, size_t off_=0)
			: blk(blk_), fit(0), off(off_<MAX_OFF_MARK? off_ : MAX_OFF_MARK), tip(tip_), tag(tag_) {}
	Entry() : Entry(MAX_ADDR) {};
};
static_assert(sizeof(Entry)==sizeof(uint64_t));

union EntryView {
	Entry e;
	uint64_t u;
};

static FORCE_INLINE bool operator==(Entry a, Entry b) noexcept {
	EntryView ta = { .e = a };
	EntryView tb = { .e = b };
	ta.e.fit = 0;
	tb.e.fit = 0;
	return ta.u == tb.u;
}
static FORCE_INLINE bool operator!=(Entry a, Entry b) noexcept {
	return !(a == b);
}

Entry FORCE_INLINE LoadAcquire(const Entry& tgt) {
	EntryView t = {
		.u = LoadAcquire((const uint64_t&)tgt)
	};
	return t.e;
}
void FORCE_INLINE StoreRelease(Entry& tgt, Entry val) {
	EntryView t = { .e = val };
	StoreRelease(((EntryView&)tgt).u, t.u);
}

static constexpr Entry CLEAN_ENTRY = Entry(MAX_ADDR);
static constexpr Entry DELETED_ENTRY = Entry(RESERVED_ADDR);

static constexpr size_t DATA_BLOCK_LIMIT = (1ULL << ADDR_BITWIDTH) - 2U;
static_assert(DATA_BLOCK_LIMIT <= RESERVED_ADDR);

static FORCE_INLINE bool IsEmpty(Entry ent) noexcept {
	return ent.blk >= RESERVED_ADDR;
}
static FORCE_INLINE bool IsClean(Entry ent) noexcept {
	return ent.blk > RESERVED_ADDR;
}

static constexpr size_t MIN_ENTRY = 256;
static constexpr size_t MAX_ENTRY = 1ULL << 34U;

static constexpr size_t DATA_RESERVE_FACTOR = 10;   // 1/DATA_RESERVE_FACTOR data is reserved clean
static constexpr size_t ENTRY_RESERVE_FACTOR = 8;   // 1/ENTRY_RESERVE_FACTOR entries are reserved clean
static constexpr size_t TotalEntry(size_t item_limit) { return item_limit*3/2; }
static constexpr size_t ItemLimit(size_t entry) { return entry*2/3; }
static_assert(ENTRY_RESERVE_FACTOR > 3);
static_assert(MAX_ENTRY < DATA_BLOCK_LIMIT / 2);
static_assert(MIN_ENTRY > ENTRY_RESERVE_FACTOR);

static constexpr size_t DATA_BLOCK_SIZE = 8;
static_assert((DATA_BLOCK_SIZE % sizeof(uint64_t)) == 0);

union RecordMark {
	struct {
		uint32_t klen : 8;
		uint32_t vlen : 24;
		uint8_t part[4];
	};
	struct {
		uint64_t klen_ : 8;
		uint64_t bcnt : 56;
	};
	uint64_t u = 0;
};

RecordMark FORCE_INLINE LoadAcquire(const RecordMark& tgt) {
	RecordMark m = {
		.u = LoadAcquire((const uint64_t&)tgt)
	};
	return m;
}
void FORCE_INLINE StoreRelease(RecordMark& tgt, RecordMark val) {
	StoreRelease(tgt.u, val.u);
}

static FORCE_INLINE RecordMark& Rc(uint8_t* block) {
	return *(RecordMark*)block;
}
static FORCE_INLINE uint8_t* RcKey(uint8_t* block) {
	return block + sizeof(uint32_t);
}
static FORCE_INLINE uint8_t* RcVal(RecordMark mark, uint8_t* block) {
	return block + sizeof(uint32_t) + mark.klen;
}
static FORCE_INLINE uint8_t* RcVal(uint8_t* block) {
	return RcVal(Rc(block), block);
}

static FORCE_INLINE bool KeyMatch(Slice key, RecordMark mark, uint8_t* block) {
	if (mark.klen != key.len) {
		return false;
	} else if (key.len == sizeof(uint64_t)) {
		return *(const uint64_t*)key.ptr == *(const uint64_t*)RcKey(block);
	} else {
		return memcmp(key.ptr, RcKey(block), key.len) == 0;
	}
}
static FORCE_INLINE bool KeyMatch(Slice key, uint8_t* block) {
	return KeyMatch(key, Rc(block), block);
}
static FORCE_INLINE bool ValMatch(Slice val, uint8_t* block) {
	return Rc(block).vlen == val.len
				 && memcmp(val.ptr, RcVal(block), val.len) == 0;
}

static FORCE_INLINE size_t RecordBlocks(size_t klen, size_t vlen) {
	assert(klen != 0);
	return ((sizeof(uint32_t)+klen+vlen)+(DATA_BLOCK_SIZE-1)) / DATA_BLOCK_SIZE;
}
static FORCE_INLINE size_t RecordBlocks(uint8_t* block) {
	return RecordBlocks(Rc(block).klen, Rc(block).vlen);
}

static FORCE_INLINE uint64_t CalcTip(uint8_t* block) {
	auto mark = Rc(block);
	return Hash(block+sizeof(uint32_t), mark.klen+mark.vlen, *(const uint32_t*)block);
}

static FORCE_INLINE RecordMark MarkForEmpty(size_t bcnt) {
	RecordMark mark;
	mark.klen = 0;
	mark.bcnt = bcnt;
	return mark;
}

#define BLK(idx) (m_data+(idx)*DATA_BLOCK_SIZE)

template <typename Func>
static FORCE_INLINE void SearchInTable(const Func& func, Entry* table, uint64_t total_entry, size_t pos, uint32_t tag) {
	const auto end = table + total_entry;
	auto ent = table + pos;
	for (size_t i = 0; i < total_entry; i++) {
		if (func(*ent, tag, i)) {
			return;
		}
		if (UNLIKELY(++ent >= end)) {
			ent = table;
		}
	}
}

template <typename Func>
static FORCE_INLINE void SearchInTable(const Func& func, uint64_t code, Entry* table, const Divisor<uint64_t>& total_entry) {
	SearchInTable(func, table, total_entry.value(), code % total_entry, CutTag(code));
}

uint64_t Estuary::touch(Slice key) const noexcept {
	auto code = Hash(key.ptr, key.len, m_const.seed);
	if (m_meta != nullptr) {
		const auto pos = code % m_const.total_entry;
		PrefetchForFuture(m_table + pos);
	}
	return code;
}

void Estuary::touch(uint64_t code) const noexcept {
	if (m_meta == nullptr) {
		return;
	}
	SearchInTable([this](Entry& ent, uint32_t tag, size_t)->bool {
		auto e = ent;
		if (IsEmpty(e)) {
			return IsClean(e);
		} else if (e.tag == tag) {
			PrefetchForFuture(BLK(e.blk));
			return true;
		}
		return false;
	}, code, (Entry*)m_table, m_const.total_entry);
}

bool Estuary::fetch(Slice key, std::string& out) const {
	if (m_meta == nullptr) {
		return false;
	}
	auto code = Hash(key.ptr, key.len, m_const.seed);
	return fetch(code, key, out);
}

//FIXME: have a very low probability of false miss
bool Estuary::fetch(uint64_t code, Slice key, std::string& out) const {
	auto done = _fetch(code, key, out);
#ifndef DISABLE_FETCH_RETRY
	//entry can be moved at most twice during sweeping, witch may cause false miss
	//NOTICE: That's not completely avoided.
	if (!done && UNLIKELY(m_lock->sweeping)) {
		done = _fetch(code, key, out);
		if (!done) {
			done = _fetch(code, key, out);
		}
	}
#endif
	return done;
}

bool Estuary::_fetch(uint64_t code, Slice key, std::string& out) const {
	bool done = false;
	SearchInTable([this, key, &out, &done](Entry& ent, uint32_t tag, size_t)->bool {
		auto e = LoadAcquire(ent);
	retry:
		if (IsEmpty(e)) {
			return IsClean(e);
		} else if (e.tag == tag) {
			auto block = BLK(e.blk);
			auto mark = LoadAcquire(Rc(block));
			auto t = LoadAcquire(ent);
			if (UNLIKELY(e != t)) {
				e = t;
				goto retry;
			}
			if (LIKELY(KeyMatch(key, mark, block))) {
				out.assign((const char*)RcVal(mark, block), mark.vlen);
				t = LoadAcquire(ent);
				if (UNLIKELY(e != t)) {
					e = t;
					goto retry;
				}
				done = true;
				return true;
			}
		}
		return false;
	}, code, (Entry*)m_table, m_const.total_entry);
	return done;
}

bool Estuary::erase(Slice key) const {
	if (m_meta == nullptr || key.ptr == nullptr || key.len == 0 || key.len > max_key_len()) {
		return {};
	}
	MutexLock master_lock(&m_lock->core);
	if (m_meta->writing) {
		throw DataException();
	}
	m_meta->writing = true;
	auto done = _erase(key);
	m_meta->writing = false;
	return done;
}

bool Estuary::_erase(Slice key) const {
	bool done = false;
	SearchInTable([this, key, &done](Entry& ent, uint32_t tag, size_t)->bool{
		const auto e = ent;
		if (IsEmpty(e)) {
			return IsClean(e);
		} else if (e.tag == tag) {
			auto block = BLK(e.blk);
			ConsistencyAssert(Rc(block).klen != 0 && Rc(block).vlen <= max_val_len());
			if (LIKELY(KeyMatch(key, block))) {
				StoreRelease(ent, DELETED_ENTRY);
				ConsistencyAssert(m_meta->item != 0);
				m_meta->item--;
				const auto bcnt = RecordBlocks(block);
				Rc(block) = MarkForEmpty(bcnt);
				m_meta->free_block += bcnt;
				ConsistencyAssert(m_meta->free_block <= m_const.total_block);
				done = true;
				return true;
			}
		}
		return false;
	}, Hash(key.ptr, key.len, m_const.seed), (Entry*)m_table, m_const.total_entry);
	return done;
}

static uint64_t FillRecord(uint8_t* block, Slice key, Slice val) {
	//mark should be updated atomically
	RecordMark mark;
	mark.klen = key.len;
	mark.vlen = val.len;
	for (unsigned i = 0; i < 4; i++) {
		if (LIKELY(key.len > 0)) {
			mark.part[i] = *key.ptr++;
			key.len--;
		} else if (val.len > 0) {
			mark.part[i] = *val.ptr++;
			val.len--;
		}
	}
	auto buf = block + 8;
	if (key.len > 0) {
		memcpy(buf, key.ptr, key.len);
		buf += key.len;
	}
	if (val.len > 0) {
		memcpy(buf, val.ptr, val.len);
	}
	StoreRelease(Rc(block), mark);
	return CalcTip(block);
}

bool Estuary::update(Slice key, Slice val) const {
	if (m_meta == nullptr
		|| key.ptr == nullptr || key.len == 0 || key.len > max_key_len()
		|| (val.len != 0 && val.ptr == nullptr) || val.len > max_val_len()) {
		return false;
	}
	MutexLock master_lock(&m_lock->core);
	if (m_meta->writing) {
		throw DataException();
	}
	m_meta->writing = true;
	auto done = _update(key, val);
	m_meta->writing = false;
	return done;
}

#define TOTAL_RESERVED_BLOCK (m_const.reserved_block + (m_const.total_block-m_const.reserved_block)/DATA_RESERVE_FACTOR)

size_t Estuary::data_free() const {
	if (m_meta == nullptr) return 0;
	ConsistencyAssert(m_meta->free_block >= TOTAL_RESERVED_BLOCK);
	return (m_meta->free_block - TOTAL_RESERVED_BLOCK) * DATA_BLOCK_SIZE;
}
size_t Estuary::item_limit() const {
	if (m_meta == nullptr) return 0;
	return ItemLimit(m_const.total_entry.value());
}

bool Estuary::_update(Slice key, Slice val) const {
	auto new_block = RecordBlocks(key.len, val.len);
	if (m_meta->free_block < new_block + TOTAL_RESERVED_BLOCK
		|| TotalEntry(m_meta->item) > m_const.total_entry.value()) {
		return false;
	}
	ConsistencyAssert(m_meta->block_cursor < m_const.total_block
		&& m_meta->free_block <= m_const.total_block
		&& m_meta->clean_entry <= m_const.total_entry.value());

	if (UNLIKELY(m_meta->clean_entry <= m_const.total_entry.value() / ENTRY_RESERVE_FACTOR)) {
		//x times random input brings 1-1/e^x coverage，x = ln(ENTRY_RESERVE_FACTOR)
		//this procedure is slow, but rarely happen

		auto table = (Entry*)m_table;
		auto& total_entry = m_const.total_entry;
		auto upstairs = [table, &total_entry, this](bool end)->bool {
			bool moved = false;
			for (size_t i = 0; i < total_entry.value(); i++) {
				if (LIKELY(IsEmpty(table[i]) || table[i].fit)) {
					continue;
				}
				auto curr = &table[i];
				size_t pos = 0;
				if (LIKELY(curr->off < MAX_OFF_MARK)) {
					if (UNLIKELY(i < curr->off)) {
						pos = total_entry.value() + i - curr->off;
					} else {
						pos = i - curr->off;
					}
				} else {
					auto block = BLK(curr->blk);
					const auto code = Hash(RcKey(block), Rc(block).klen, m_const.seed);
					ConsistencyAssert(curr->tag == CutTag(code));
					pos = code % total_entry;
				}
				bool fit = true;
				SearchInTable([&fit, &moved, curr, end](Entry& ent, uint32_t tag, size_t off)->bool{
					if (IsEmpty(ent)) {
						moved = true;
						ConsistencyAssert(!IsClean(ent));
						ent = *curr;
						ent.off = std::min(off, MAX_OFF_MARK);
						if (fit) {
							ent.fit = 1;
						}
						StoreRelease(*curr, DELETED_ENTRY);
						if (end) {
							curr->fit = 1;
						}
						return true;
					} else if (!ent.fit) {
						if (&ent == curr) {
							if (fit) {
								curr->fit = 1;
							}
							return true;
						}
						fit = false;
					}
					return false;
				}, table, total_entry.value(), pos, curr->tag);
			}
			return moved;
		};

		//entry can be moved twice at most
		m_lock->sweeping = true;
		MemoryBarrier();
		if (upstairs(false)) {
			upstairs(true);
		}

		size_t dirty = 0;
		size_t item = 0;
		for (size_t i = 0; i < total_entry.value(); i++) {
			if (IsEmpty(table[i])) {
				if (table[i].fit) {
					dirty++;
					table[i].fit = 0;
				} else {
					table[i] = CLEAN_ENTRY;
				}
			} else {
				item++;
				table[i].fit = 0;
			}
		}

		//keep sweeping status longer
		sched_yield();
		MemoryBarrier();
		m_lock->sweeping = false;

		ConsistencyAssert(item == m_meta->item);
		m_meta->clean_entry = total_entry.value() - item - dirty;
	}

	auto& cur = m_meta->block_cursor;
	ConsistencyAssert(Rc(BLK(cur)).klen == 0 && cur+Rc(BLK(cur)).bcnt <= m_const.total_block);

	const auto code = Hash(key.ptr, key.len, m_const.seed);
	auto origin_entry = CLEAN_ENTRY;
	//update after movement may cause ABA problem, detect and fix it

	auto move_record = [this, &cur, code, key, &origin_entry](size_t vic) {
		assert(Rc(BLK(vic)).klen != 0);
		const auto bcnt = RecordBlocks(BLK(vic));
		memcpy(BLK(cur)+sizeof(RecordMark), BLK(vic)+sizeof(RecordMark), bcnt*DATA_BLOCK_SIZE-sizeof(RecordMark));
		const auto bcode = Hash(RcKey(BLK(vic)), Rc(BLK(vic)).klen, m_const.seed);
		Entry* pent = nullptr;
		if (UNLIKELY(bcode == code && KeyMatch(key, BLK(vic)))) {
			pent = &origin_entry;
			ConsistencyAssert(IsClean(origin_entry));
		}
		bool done = false;
		SearchInTable([this, &cur, vic, bcnt, &done, pent](Entry& ent, uint32_t tag, size_t off)->bool{
			auto e = ent;
			if (IsEmpty(e)) {
				return IsClean(e);
			} else if (e.blk == vic) {
				if (UNLIKELY(pent != nullptr)) {
					*pent = e;
				}
				m_meta->free_block -= bcnt;
				auto next = cur + bcnt;
				if (LIKELY(next != m_const.total_block)) {
					ConsistencyAssert(next < m_const.total_block);
					Rc(BLK(next)) = MarkForEmpty(Rc(BLK(cur)).bcnt-bcnt);
				}
				Rc(BLK(cur)) = Rc(BLK(vic));
				e.blk = cur;
				StoreRelease(ent, e);
				Rc(BLK(vic)) = MarkForEmpty(bcnt);
				cur = next;
				m_meta->free_block += bcnt;
				done = true;
				return true;
			}
			return false;
		}, bcode, (Entry*)m_table, m_const.total_entry);
		if (UNLIKELY(!done)) {
			Rc(BLK(vic)) = MarkForEmpty(bcnt);
			m_meta->free_block += bcnt;
			ConsistencyAssert(m_meta->free_block <= m_const.total_block);
		}
	};

	//defragmentation
	ConsistencyAssert(Rc(BLK(cur)).bcnt >= m_const.reserved_block);
	bool overflow = false;
	while (Rc(BLK(cur)).bcnt < new_block + m_const.reserved_block) {
		auto nxt = cur + Rc(BLK(cur)).bcnt;
		if (UNLIKELY(nxt == m_const.total_block)) {
			ConsistencyAssert(!overflow && m_meta->free_block >= Rc(BLK(cur)).bcnt);
			overflow = true; //no more than once
			size_t vic = 0;
			while (vic < cur) {	//some blocks may be moved more than once
				if (Rc(BLK(vic)).klen == 0) {
					vic += Rc(BLK(vic)).bcnt;
				} else if (vic < new_block + m_const.reserved_block) {
					const auto bcnt = RecordBlocks(BLK(vic));
					if (Rc(BLK(cur)).bcnt < bcnt) {
						break;
					}
					move_record(vic);
					vic += bcnt;
					if (UNLIKELY(cur == m_const.total_block)) {
						break;
					}
				} else {
					break;
				}
			}
			ConsistencyAssert(vic <= cur);
			Rc(m_data) = MarkForEmpty(vic);
			cur = 0;
		} else {
			size_t bcnt;
			if (Rc(BLK(nxt)).klen == 0) {
				ConsistencyAssert(nxt+Rc(BLK(nxt)).bcnt <= m_const.total_block);
				bcnt = Rc(BLK(nxt)).bcnt;
			} else { //reserved_block must be enough
				bcnt = RecordBlocks(BLK(nxt));
				ConsistencyAssert(bcnt <= Rc(BLK(cur)).bcnt);
				move_record(nxt);
			}
			Rc(BLK(cur)).bcnt += bcnt;
		}
	}

	m_meta->free_block -= new_block;
	const auto next = cur + new_block;
	Rc(BLK(next)) = MarkForEmpty(Rc(BLK(cur)).bcnt-new_block);
	Rc(BLK(cur)).bcnt = new_block;
	const auto neo = cur;
	cur = next;
	auto tip = FillRecord(BLK(neo), key, val);

	bool done = false;
	SearchInTable([this, &cur, neo, tip, key, val, &done, origin_entry](Entry& ent, uint32_t tag, size_t off)->bool{
		const auto e = ent;
		if (IsEmpty(e)) {
			if (IsClean(e)) {
				m_meta->clean_entry--;
			}
			StoreRelease(ent, Entry(neo, tip, tag, off));
			m_meta->item++;
			done = true;
			return true;
		} else if (e.tag == tag) {
			auto block = BLK(e.blk);
			ConsistencyAssert(Rc(block).klen != 0 && Rc(block).vlen <= max_val_len());
			if (LIKELY(KeyMatch(key, block))) {
				const auto bcnt = RecordBlocks(block);
				if (UNLIKELY(ValMatch(val, block))) {	//rollback
					Rc(BLK(neo)) = MarkForEmpty(bcnt);
					const auto tail = Rc(BLK(cur)).bcnt;
					cur = neo;
					Rc(BLK(neo)) = MarkForEmpty(bcnt+tail);
				} else {
					Entry entry(neo, tip, tag, off);
					if (UNLIKELY(entry == origin_entry)) {		//ABA after movement
						entry.tip ^= 1;
					}
					StoreRelease(ent, entry);
					Rc(block) = MarkForEmpty(bcnt);
				}
				m_meta->free_block += bcnt;
				ConsistencyAssert(m_meta->free_block <= m_const.total_block);
				done = true;
				return true;
			}
		}
		return false;
	}, code, (Entry*)m_table, m_const.total_entry);
	return done;
}

static bool InitLock(Estuary::Lock* lock, bool shared=true) {
	const int pshared = shared? PTHREAD_PROCESS_SHARED : PTHREAD_PROCESS_PRIVATE;
	pthread_mutexattr_t mutexattr;
	if (pthread_mutexattr_init(&mutexattr) != 0
			|| pthread_mutexattr_setpshared(&mutexattr, pshared) != 0
			|| pthread_mutex_init(&lock->core, &mutexattr) != 0) {
		//pthread_mutexattr_destroy is unnecessary
		return false;
	}
	lock->sweeping = false;
	return true;
}

Estuary::~Estuary() noexcept {
	if (m_meta == nullptr) {
		return;
	}
	if (m_monopoly_extra != nullptr) {
		pthread_mutex_destroy(&m_lock->core);
	}
}

Estuary Estuary::Load(const std::string& path, LoadPolicy policy) {
  Estuary out;
  MemMap res;
  switch (policy) {
    case SHARED:
      res = MemMap(path.c_str(), true, false);
      break;
    case MONOPOLY:
      res = MemMap(path.c_str(), true, true);
      break;
    case COPY_DATA:
      res = MemMap(path.c_str(), MemMap::load_by_copy);
      break;
    default:
      return out;
  }
  if (!!res) {
    out._init(std::move(res), policy!=SHARED, path.c_str());
  }
  return out;
}

Estuary Estuary::Load(size_t size, const std::function<bool(uint8_t*)>& load) {
  Estuary out;
  MemMap res(size, load);
  if (!!res) {
    out._init(std::move(res), true, "...");
  }
  return out;
}

void Estuary::_init(MemMap&& res, bool monopoly, const char* path) {
	if (!res || res.size() < sizeof(Header)) {
		return;
	}
	auto meta = (Header*)res.addr();
	auto lock_off = sizeof(Header);
	auto table_off = ((lock_off+sizeof(Lock)) & ~(sizeof(uintptr_t)-1ULL)) + sizeof(uintptr_t);
	auto data_off = table_off + meta->total_entry * sizeof(Entry);
	if (meta->magic != MAGIC
		|| meta->total_entry < MIN_ENTRY || meta->total_entry > MAX_ENTRY
		|| meta->total_block < meta->total_entry || meta->total_block > DATA_BLOCK_LIMIT
		|| res.size() < data_off + meta->total_block * DATA_BLOCK_SIZE) {
    Logger::Printf("broken file: %s\n", path);
		return;
	}

	std::unique_ptr<uint8_t[]> monopoly_extra;
	auto lock = (Lock*)(res.addr() + lock_off);
	if (monopoly) {
		if (meta->writing) {
			Logger::Printf("file is not saved correctly: %s\n", path);
			return;
		}
		monopoly_extra = std::make_unique<uint8_t[]>(sizeof(Lock));
		lock = (Lock*)monopoly_extra.get();
		if (!InitLock(lock)) {
			Logger::Printf("fail to reset locks in: %s\n", path);
			return;
		}
	}

	m_lock = lock;
	m_table = (uint64_t*)(res.addr()+table_off);
	m_data = res.addr()+data_off;
	auto& mark = *(RecordMark*)&meta->kv_limit;
	m_const.max_key_len = mark.klen;
	m_const.max_val_len = mark.vlen;
	m_const.reserved_block = RecordBlocks(mark.klen, mark.vlen) * 2;
	m_const.seed = meta->seed;
	m_const.total_entry = meta->total_entry;
	m_const.total_block = meta->total_block;
	if (m_const.total_block <= m_const.reserved_block) {
		return;
	}
	m_monopoly_extra = std::move(monopoly_extra);
	m_resource = std::move(res);
	m_meta = meta;
}

bool Estuary::Create(const std::string& path, const Config& config, IDataReader* source) {
	if (TotalEntry(config.item_limit) < MIN_ENTRY || TotalEntry(config.item_limit) > MAX_ENTRY
		|| config.max_key_len == 0 || config.max_key_len > MAX_KEY_LEN
		|| config.max_val_len == 0 || config.max_val_len > MAX_VAL_LEN
		|| config.avg_size_per_item < 2 || config.avg_size_per_item > config.max_key_len+config.max_val_len) {
		Logger::Printf("bad arguments\n");
		return false;
	}
	Header header;
	((RecordMark*)&header.kv_limit)->klen = config.max_key_len;
	((RecordMark*)&header.kv_limit)->vlen = config.max_val_len;
	header.seed = GetSeed();

	static_assert(sizeof(Header)%sizeof(uintptr_t) == 0, "alignment check");

	header.total_entry = TotalEntry(config.item_limit);
	header.clean_entry = header.total_entry;
	auto block_per_item = ((config.avg_size_per_item+sizeof(uint32_t))+(DATA_BLOCK_SIZE-1))/DATA_BLOCK_SIZE;
	header.total_block = block_per_item * (config.item_limit + 1);
	const auto init_end = header.total_block;
	header.total_block += header.total_block / (DATA_RESERVE_FACTOR-1) + 1;
	header.total_block += RecordBlocks(config.max_key_len, config.max_val_len) * 2;
	if (header.total_block > DATA_BLOCK_LIMIT) {
		Logger::Printf("too big\n");
		return false;
	}
	header.free_block = header.total_block;

	size_t size = sizeof(header);
	const auto lock_off = size;
	size += sizeof(Lock);
	size = (size & ~(sizeof(uintptr_t)-1ULL)) + sizeof(uintptr_t);
	const auto table_off = size;
	size += header.total_entry * sizeof(Entry);
	const auto data_off = size;
	size += header.total_block * DATA_BLOCK_SIZE;

	MemMap res(path.c_str(), false, true, size);
	if (!res) {
		return false;
	}
	auto meta = (Header*)res.addr();
	auto lock = (Lock*)(res.addr()+lock_off);
	auto table = (uint64_t*)(res.addr()+table_off);
	auto data = res.addr() + data_off;

	auto blk = [data](size_t idx)->uint8_t* {
		return data + idx*DATA_BLOCK_SIZE;
	};

	*meta = header;
	if (!InitLock(lock)) {
		Logger::Printf("fail to init\n");
		return false;
	}
	for (size_t i = 0; i < header.total_entry; i++) {
		*(Entry*)(table+i) = CLEAN_ENTRY;
	}

	if (source != nullptr) {
		Divisor<uint64_t> total_entry(header.total_entry);
		source->reset();
		auto total = source->total();
		if (total > config.item_limit) {
			Logger::Printf("too many items\n");
			return false;
		}
		for (size_t i = 0; i < total; i++) {
			auto rec = source->read();
			if (rec.key.ptr == nullptr || rec.key.len == 0 || rec.key.len > config.max_key_len
				|| (rec.val.len != 0 && rec.val.ptr == nullptr) || rec.val.len > config.max_val_len) {
				Logger::Printf("broken item\n");
				return false;
			}
			bool done = false;
			SearchInTable([&rec, meta, &blk, init_end, &done](Entry& ent, uint32_t tag, size_t off)->bool{
					const auto e = ent;
					if (IsEmpty(e)) {
						meta->item++;
						meta->clean_entry--;
					} else if (e.tag == tag && KeyMatch(rec.key, blk(e.blk))) {
						const auto bcnt = RecordBlocks(blk(e.blk));
						Rc(blk(e.blk)) = MarkForEmpty(bcnt);
						meta->free_block += bcnt;
					} else {
						return false;
					}
					auto bcnt = RecordBlocks(rec.key.len, rec.val.len);
					auto block = blk(meta->block_cursor);
					ent = Entry(meta->block_cursor, 0, tag, off);
					meta->block_cursor += bcnt;
					if (meta->block_cursor > init_end) {
						Logger::Printf("out of data capacity\n");
						return true;
					}
					meta->free_block -= bcnt;
					Rc(block).klen = rec.key.len;
					Rc(block).vlen = rec.val.len;
					memcpy(RcKey(block), rec.key.ptr, rec.key.len);
					memcpy(RcVal(block), rec.val.ptr, rec.val.len);
					ent.tip = CalcTip(block);
					done = true;
					return true;
				}, Hash(rec.key.ptr, rec.key.len, header.seed), (Entry*)table, total_entry);
			if (UNLIKELY(!done)) {
				return false;
			}
		}
	}

	Rc(blk(meta->block_cursor)) = MarkForEmpty(meta->total_block - meta->block_cursor);
	return true;
}

} //estuary
