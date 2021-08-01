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
#include "spin_rwlock.h"

namespace estuary {

#ifndef USE_PTHREAD_SPIN_LOCK
struct MicroMutex {
#ifndef USE_UNFAIR_SPIN_LOCK
	SpinRWLock core;
	static constexpr uint8_t TYPE = 0;
#else
	UnfairSpinRWLock core;
	static constexpr uint8_t TYPE = 2;
#endif

	static int Init(MicroMutex* mtx, int) {
		mtx->core.init();
		return 0;
	}
	static int LockShared(MicroMutex* mtx) {
		mtx->core.read_lock();
		return 0;
	}
	static int UnlockShared(MicroMutex* mtx) {
		mtx->core.read_unlock();
		return 0;
	}
	static int Lock(MicroMutex* mtx) {
		mtx->core.write_lock();
		return 0;
	}
	static int Unlock(MicroMutex* mtx) {
		mtx->core.write_unlock();
		return 0;
	}
};

struct _ReadLock {
	using LockType = MicroMutex;
	static constexpr auto Lock = MicroMutex::LockShared;
	static constexpr auto Unlock = MicroMutex::UnlockShared;
};
using ReadLock = LockGuard<_ReadLock>;

struct _WriteLock {
	using LockType = MicroMutex;
	static constexpr auto Lock = MicroMutex::Lock;
	static constexpr auto Unlock = MicroMutex::Unlock;
};
using WriteLock = LockGuard<_WriteLock>;

#else
struct MicroMutex {
	pthread_spinlock_t core;
	static constexpr uint8_t TYPE = 1;

	static int Init(MicroMutex* mtx, int mode) {
		return pthread_spin_init(&mtx->core, mode);
	}
	static int Lock(MicroMutex* mtx) {
		return pthread_spin_lock(&mtx->core);
	}
	static int Unlock(MicroMutex* mtx) {
		return pthread_spin_unlock(&mtx->core);
	}
};

struct _MicroLock {
	using LockType = MicroMutex;
	static constexpr auto Lock = MicroMutex::Lock;
	static constexpr auto Unlock = MicroMutex::Unlock;
};
using ReadLock = LockGuard<_MicroLock>;
using WriteLock = LockGuard<_MicroLock>;
#endif

static constexpr uint16_t MAGIC = 0xE999;
struct Estuary::Meta {
	uint16_t magic = MAGIC;
	uint16_t lock_mask = 0;
	uint32_t kv_limit = 0;
	uint32_t seed = 0;
	bool writing = false;
	uint8_t lock_type = MicroMutex::TYPE;
	uint16_t reference = 0;
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

struct Estuary::Locks {
	pthread_mutex_t master;
	MicroMutex pool[0];
};

static constexpr size_t DATA_BLOCK_SIZE = 8;
static_assert((DATA_BLOCK_SIZE % sizeof(uint64_t)) == 0);

static constexpr size_t MIN_ENTRY = 256;
static constexpr size_t MAX_ENTRY = 1ULL << 34U;

static constexpr unsigned ADDR_BITWIDTH = 43;
static constexpr size_t DATA_BLOCK_LIMIT = (1ULL << ADDR_BITWIDTH) - 2U;

static constexpr size_t DATA_RESERVE_FACTOR = 10;   // 1/DATA_RESERVE_FACTOR data is reserved clean
static constexpr size_t ENTRY_RESERVE_FACTOR = 8;   // 1/ENTRY_RESERVE_FACTOR entries are reserved clean
static constexpr size_t TotalEntry(size_t item_limit) { return item_limit*3/2; }
static constexpr size_t ItemLimit(size_t entry) { return entry*2/3; }
static_assert(ENTRY_RESERVE_FACTOR > 3);
static_assert(MAX_ENTRY < DATA_BLOCK_LIMIT / 2);
static_assert(MIN_ENTRY > ENTRY_RESERVE_FACTOR);

const char* LockException::what() const noexcept {
	return "fail to handle lock";
}
const char* DataException::what() const noexcept {
	return "broken data";
}

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
};

static FORCE_INLINE RecordMark& Rc(uint8_t* block) {
	return *(RecordMark*)block;
}
static FORCE_INLINE uint8_t* RcKey(uint8_t* block) {
	return block + sizeof(uint32_t);
}
static FORCE_INLINE uint8_t* RcVal(uint8_t* block) {
	return block + sizeof(uint32_t) + Rc(block).klen;
}

static FORCE_INLINE bool KeyMatch(Slice key, uint8_t* block) {
	if (Rc(block).klen != key.len) {
		return false;
	} else if (key.len == sizeof(uint64_t)) {
		return *(const uint64_t*)key.ptr == *(const uint64_t*)RcKey(block);
	} else {
		return memcmp(key.ptr, RcKey(block), key.len) == 0;
	}
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

static_assert(ADDR_BITWIDTH < 63U);
static constexpr unsigned TAG_BITWIDTH = 63U - ADDR_BITWIDTH;

struct Entry {
	uint64_t blk : ADDR_BITWIDTH;
	uint64_t fit : 1;
	uint64_t tag : TAG_BITWIDTH;
	Entry() = default;
	explicit constexpr Entry(uint64_t b, uint64_t t=(1UL<<TAG_BITWIDTH)-1U) : blk(b), fit(0), tag(t) {}
	FORCE_INLINE void store_release(Entry e) const noexcept {
		union {
			Entry e;
			uint64_t u;
		} t = { .e = e };
		StoreRelease(*(uint64_t*)this, t.u);
	}
	FORCE_INLINE void load_relaxed(const Entry& e) noexcept {
		union {
			Entry e;
			uint64_t u;
		} t = { .u = LoadRelaxed(*(const uint64_t*)&e) };
		*this = t.e;
	}
};
static_assert(sizeof(Entry)==sizeof(uint64_t));

static constexpr uint64_t MAX_ADDR = (1ULL << ADDR_BITWIDTH) - 1U;
static constexpr uint64_t RESERVED_ADDR = (1ULL << ADDR_BITWIDTH) - 2U;
static constexpr Entry CLEAN_ENTRY = Entry(MAX_ADDR);
static constexpr Entry DELETED_ENTRY = Entry(RESERVED_ADDR);

static_assert(DATA_BLOCK_LIMIT <= RESERVED_ADDR);

static FORCE_INLINE bool IsEmpty(Entry ent) noexcept {
	return ent.blk >= RESERVED_ADDR;
}
static FORCE_INLINE bool IsClean(Entry ent) noexcept {
	return ent.blk > RESERVED_ADDR;
}

static_assert(TAG_BITWIDTH >= 16U && TAG_BITWIDTH <= 32U);
#define GET_LOCK(tag) (m_locks->pool+((tag)&m_const.lock_mask))

#define BLK(idx) (m_data+(idx)*DATA_BLOCK_SIZE)

template <typename Func>
static FORCE_INLINE void SearchInTable(const Func& func, uint64_t code, Entry* table, const Divisor<uint64_t>& total_entry) {
	const uint32_t tag = code >> (64U - TAG_BITWIDTH);
	const auto end = table + total_entry.value();
	const auto pos = code % total_entry;
	auto ent = table + pos;
	for (size_t i = 0; i < total_entry.value(); i++) {
		if (func(*ent, tag)) {
			return;
		}
		if (UNLIKELY(++ent >= end)) {
			ent = table;
		}
	}
}

bool Estuary::fetch(Slice key, std::string& out) const {
	auto code = Hash(key.ptr, key.len, m_const.seed);
	return fetch(code, key, out);
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
	SearchInTable([this](Entry& ent, uint32_t tag)->bool {
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

//FIXME: have a very low probability of false miss
bool Estuary::fetch(uint64_t code, Slice key, std::string& out) const {
	if (m_meta == nullptr) {
		return false;
	}
	out.clear();
	auto ret = _fetch(code, key, out);
#ifndef DISABLE_FETCH_RETRY
	//entry can be moved at most twice during sweeping, witch may cause false miss
	//NOTICE: That's not completely avoided.
	if (ret < 0 || (ret > 0 && UNLIKELY(LoadRelaxed(*m_sweeping)))) {
		ret = _fetch(code, key, out);
		if (ret < 0 || (ret > 0 && UNLIKELY(LoadRelaxed(*m_sweeping)))) {
			ret = _fetch(code, key, out);
		}
	}
#endif
	return ret == 0;
}

//0=found, 1=miss, -1=retry
int Estuary::_fetch(uint64_t code, Slice key, std::string& out) const {
	struct {
		uint32_t tag = 0;
		uint32_t val_len = UINT32_MAX;
		const Entry* ent = nullptr;
	} snapshot;
	static_assert(UINT32_MAX > MAX_VAL_LEN);
	SearchInTable([this, key, &snapshot, &out](Entry& ent, uint32_t tag)->bool{
		auto e = ent;
		if (IsEmpty(e)) {
			return IsClean(e);
		} else if (e.tag == tag) {
			PrefetchForNext(BLK(e.blk));
			ReadLock lk(GET_LOCK(tag));
			e.load_relaxed(ent);
			auto block = BLK(e.blk);
			if (UNLIKELY(IsEmpty(e))) {
				return IsClean(e);
			} else if (LIKELY(e.tag == tag && KeyMatch(key, block))) {
				snapshot.val_len = Rc(block).vlen;
				if (out.capacity() < snapshot.val_len) {
					snapshot.ent = &ent;
					snapshot.tag = tag;
				} else {
					out.assign((const char*)RcVal(block), snapshot.val_len);
				}
				return true;
			}
		}
		return false;
	}, code, (Entry*)m_table, m_const.total_entry);
	if (snapshot.ent == nullptr) {
		return snapshot.val_len > MAX_VAL_LEN? 1 : 0;
	}
	//target entry is found, it never move without sweeping
	for (Entry e = CLEAN_ENTRY;;) {
		out.reserve(snapshot.val_len);
		ReadLock lk(GET_LOCK(snapshot.tag));
		e.load_relaxed(*snapshot.ent);
		auto block = BLK(e.blk);
		if (LIKELY(!IsEmpty(e) && e.tag == snapshot.tag && KeyMatch(key, block))) {
			snapshot.val_len = Rc(block).vlen;
			if (UNLIKELY(out.capacity() < snapshot.val_len)) {
				continue;  //key is unchanged, but value is modified
			}
			out.assign((const char*)RcVal(block), snapshot.val_len);
			return 0;
		}
		return -1;
	}
}

static FORCE_INLINE RecordMark MarkForEmpty(size_t bcnt) {
	RecordMark mark;
	mark.klen = 0;
	mark.bcnt = bcnt;
	return mark;
}

static FORCE_INLINE void UpdateEntry(MicroMutex* lock, Entry& ent, const Entry val) {
	WriteLock _(lock);
	ent = val;
}

bool Estuary::erase(Slice key) const {
	if (m_meta == nullptr || key.ptr == nullptr || key.len == 0 || key.len > max_key_len()) {
		return {};
	}
	MutexLock master_lock(&m_locks->master);
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
	SearchInTable([this, key, &done](Entry& ent, uint32_t tag)->bool{
			const auto e = ent;
			if (IsEmpty(e)) {
				return IsClean(e);
			} else if (e.tag == tag) {
				auto block = BLK(e.blk);
				ConsistencyAssert(Rc(block).klen != 0 && Rc(block).vlen <= max_val_len());
				if (LIKELY(KeyMatch(key, block))) {
					UpdateEntry(GET_LOCK(tag), ent, DELETED_ENTRY);
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

static void FillRecord(uint8_t* block, Slice key, Slice val) {
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
	Rc(block) = mark;
}

bool Estuary::update(Slice key, Slice val) const {
	if (m_meta == nullptr
		|| key.ptr == nullptr || key.len == 0 || key.len > max_key_len()
		|| (val.len != 0 && val.ptr == nullptr) || val.len > max_val_len()) {
		return false;
	}
	MutexLock master_lock(&m_locks->master);
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
		//TODO: need better algorithm

		auto get_hash_code = [this](Entry entry)->uint64_t {
			auto block = BLK(entry.blk);
			const auto code = Hash(RcKey(block), Rc(block).klen, m_const.seed);
			ConsistencyAssert(entry.tag == (code>>(64U - TAG_BITWIDTH)));
			return code;
		};

		auto table = (Entry*)m_table;
		auto& total_entry = m_const.total_entry;
		auto upstairs = [table, &total_entry, &get_hash_code](bool end)->bool {
			bool moved = false;
			for (size_t i = 0; i < total_entry.value(); i++) {
				if (LIKELY(IsEmpty(table[i]) || table[i].fit)) {
					continue;
				}
				bool fit = true;
				auto curr = &table[i];
				SearchInTable([&fit, &moved, curr, end](Entry& ent, uint32_t tag)->bool{
					if (IsEmpty(ent)) {
						moved = true;
						ConsistencyAssert(!IsClean(ent));
						ent = *curr;
						if (fit) {
							ent.fit = 1;
						}
						curr->store_release(DELETED_ENTRY);
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
				}, get_hash_code(*curr), table, total_entry);
			}
			return moved;
		};

		//entry can be moved twice at most
		*m_sweeping = true;
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
		MemoryBarrier();
		*m_sweeping = false;

		ConsistencyAssert(item = m_meta->item);
		m_meta->clean_entry = total_entry.value() - item - dirty;
	}

	auto& cur = m_meta->block_cursor;
	ConsistencyAssert(Rc(BLK(cur)).klen == 0 && cur+Rc(BLK(cur)).bcnt <= m_const.total_block);

	auto move_record = [this, &cur](size_t vic) {
		assert(Rc(BLK(vic)).klen != 0);
		const auto bcnt = RecordBlocks(BLK(vic));
		memcpy(BLK(cur)+sizeof(RecordMark), BLK(vic)+sizeof(RecordMark), bcnt*DATA_BLOCK_SIZE-sizeof(RecordMark));
		bool done = false;
		SearchInTable([this, &cur, vic, bcnt, &done](Entry& ent, uint32_t tag)->bool{
				const auto e = ent;
				if (IsEmpty(e)) {
					return IsClean(e);
				} else if (e.blk == vic) {
					m_meta->free_block -= bcnt;
					auto next = cur + bcnt;
					if (LIKELY(next != m_const.total_block)) {
						ConsistencyAssert(next < m_const.total_block);
						Rc(BLK(next)) = MarkForEmpty(Rc(BLK(cur)).bcnt-bcnt);
					}
					Rc(BLK(cur)) = Rc(BLK(vic));
					UpdateEntry(GET_LOCK(tag), ent, Entry(cur,tag));
					Rc(BLK(vic)) = MarkForEmpty(bcnt);
					cur = next;
					m_meta->free_block += bcnt;
					done = true;
					return true;
				}
				return false;
			}, Hash(RcKey(BLK(vic)), Rc(BLK(vic)).klen, m_const.seed), (Entry*)m_table, m_const.total_entry);
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
	FillRecord(BLK(neo), key, val);

	bool done = false;
	SearchInTable([this, &cur, neo, key, val, &done](Entry& ent, uint32_t tag)->bool{
			const auto e = ent;
			if (IsEmpty(e)) {
				if (IsClean(e)) {
					m_meta->clean_entry--;
				}
				ent.store_release(Entry(neo, tag));
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
						UpdateEntry(GET_LOCK(tag), ent, Entry(neo, tag));
						Rc(block) = MarkForEmpty(bcnt);
					}
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

static bool InitLocks(Estuary::Locks* locks, uint16_t mask, bool shared=true) {
	const int mode = shared? PTHREAD_PROCESS_SHARED : PTHREAD_PROCESS_PRIVATE;
	pthread_mutexattr_t mutexattr;
	if (pthread_mutexattr_init(&mutexattr) != 0
		|| pthread_mutexattr_setpshared(&mutexattr, mode) != 0
		|| pthread_mutex_init(&locks->master, &mutexattr) != 0) {
		//pthread_mutexattr_destroy is unnecessary
		return false;
	}
	for (unsigned i = 0; i <= mask; i++) {
		MicroMutex::Init(&locks->pool[i], mode);
	}
	return true;
}

Estuary::~Estuary() noexcept {
	if (m_meta == nullptr) {
		return;
	}
	if (m_monopoly_extra != nullptr) {
		m_meta->reference = 0;
		pthread_mutex_destroy(&m_locks->master);
	} else {
		SubRelaxed(m_meta->reference, (uint16_t)1U);
	}
}

static FORCE_INLINE size_t LocksSize(uint16_t mask) {
	return sizeof(Estuary::Locks) + (mask+1U) * sizeof(MicroMutex);
}

static FORCE_INLINE uint16_t CalcLockMask(unsigned concurrency) {
	if (concurrency < 1) {
		concurrency = 1;
	} else if (concurrency > 512) {
		concurrency = 512;
	}
	auto n = 1U << (32U-__builtin_clz(concurrency-1));
	assert(n > 0);
	static_assert(512U%sizeof(MicroMutex) == 0 && sizeof(MicroMutex) >= 4);
	return n*(512U/sizeof(MicroMutex)) - 1;
}

Estuary Estuary::Load(const std::string& path, LoadPolicy policy, unsigned concurrency) {
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
	if (!res || res.size() < sizeof(Header)) {
		return out;
	}
	auto meta = (Header*)res.addr();
	auto locks_off = sizeof(Header);
	auto sweeping_off = locks_off + LocksSize(meta->lock_mask);
	auto table_off = (sweeping_off & ~(sizeof(uintptr_t)-1ULL)) + sizeof(uintptr_t);
	auto data_off = table_off + meta->total_entry * sizeof(Entry);
	if (meta->magic != MAGIC || meta->lock_type != MicroMutex::TYPE
		|| (meta->lock_mask & (meta->lock_mask+1U)) != 0
		|| meta->total_entry < MIN_ENTRY || meta->total_entry > MAX_ENTRY
		|| meta->total_block < meta->total_entry || meta->total_block > DATA_BLOCK_LIMIT
		|| res.size() < data_off + meta->total_block * DATA_BLOCK_SIZE) {
		Logger::Printf("broken file: %s\n", path.c_str());
		return out;
	}

	std::unique_ptr<uint8_t[]> monopoly_extra;
	auto locks = (Locks*)(res.addr()+locks_off);
	auto sweeping = (bool*)(res.addr()+sweeping_off);
	auto lock_mask = meta->lock_mask;
	if (policy != SHARED) {
		if (meta->writing) {
			Logger::Printf("file is not saved correctly: %s\n", path.c_str());
			return out;
		}
		if (concurrency != 0) {
			lock_mask = CalcLockMask(concurrency);
		}
		auto locks_size = LocksSize(lock_mask);
		monopoly_extra = std::make_unique<uint8_t[]>(locks_size + sizeof(bool));
		locks = (Locks*)monopoly_extra.get();
		if (!InitLocks(locks, lock_mask, false)) {
			Logger::Printf("fail to reset locks in: %s\n", path.c_str());
			return out;
		}
		sweeping = (bool*)(monopoly_extra.get()+locks_size);
		*sweeping = false;
		meta->reference = UINT16_MAX;
	} else if (AddRelaxed(meta->reference, (uint16_t)1U) >= UINT16_MAX / 2) {
		SubRelaxed(meta->reference, (uint16_t)1U);
		Logger::Printf("too many reference: %s\n", path.c_str());
		return out;
	}

	out.m_locks = locks;
	out.m_sweeping = sweeping;
	out.m_table = (uint64_t*)(res.addr()+table_off);
	out.m_data = res.addr()+data_off;
	out.m_const.lock_mask = lock_mask;
	auto& mark = *(RecordMark*)&meta->kv_limit;
	out.m_const.max_key_len = mark.klen;
	out.m_const.max_val_len = mark.vlen;
	out.m_const.reserved_block = RecordBlocks(mark.klen, mark.vlen) * 2;
	out.m_const.seed = meta->seed;
	out.m_const.total_entry = meta->total_entry;
	out.m_const.total_block = meta->total_block;
	if (out.m_const.total_block <= out.m_const.reserved_block) {
		return out;
	}
	out.m_monopoly_extra = std::move(monopoly_extra);
	out.m_resource = std::move(res);
	out.m_meta = meta;
	return out;
}

bool Estuary::ResetLocks(const std::string& path) {
	Estuary out;
	MemMap res(path.c_str(), false, true);
	if (!res || res.size() < sizeof(Header)) {
		return false;
	}
	auto meta = (Header*)res.addr();
	auto locks_off = sizeof(Header);
	if (meta->magic != MAGIC || meta->lock_type != MicroMutex::TYPE
		|| (meta->lock_mask & (meta->lock_mask+1U)) != 0
		|| res.size() < locks_off + LocksSize(meta->lock_mask)) {
		Logger::Printf("broken file: %s\n", path.c_str());
		return false;
	}
	if (meta->writing) {
		Logger::Printf("file is not saved correctly: %s\n", path.c_str());
		return false;
	}
	if (meta->reference != 0) {
		meta->reference = 0;
		Logger::Printf("unreleased references in: %s\n", path.c_str());
	}
	auto locks = (Locks*)(res.addr() + locks_off);
	if (!InitLocks(locks, meta->lock_mask)) {
		Logger::Printf("fail to reset locks in: %s\n", path.c_str());
		return false;
	}
	return true;
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
	header.lock_mask = CalcLockMask(config.concurrency);
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
	const auto locks_off = size;
	size += LocksSize(header.lock_mask);
	const auto sweeping_off = size;
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
	auto locks = (Locks*)(res.addr()+locks_off);
	memset(res.addr()+sweeping_off, 0, table_off-sweeping_off);
	auto table = (uint64_t*)(res.addr()+table_off);
	auto data = res.addr() + data_off;

	auto blk = [data](size_t idx)->uint8_t* {
		return data + idx*DATA_BLOCK_SIZE;
	};

	*meta = header;
	if (!InitLocks(locks, header.lock_mask)) {
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
			SearchInTable([&rec, meta, &blk, init_end, &done](Entry& ent, uint32_t tag)->bool{
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
					ent = Entry(meta->block_cursor, tag);
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