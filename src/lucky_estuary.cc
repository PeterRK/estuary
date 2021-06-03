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
#include <cstring>
#include <tuple>
#include <thread>
#include <chrono>
#include <algorithm>
#include <pthread.h>
#include <lucky_estuary.h>
#include "internal.h"

namespace estuary {

static constexpr uint16_t MAGIC = 0xE888;
struct LuckyEstuary::Meta {
	uint16_t magic = MAGIC;
	bool writing = false;
	uint8_t key_len = 0;
	uint32_t val_len = 0;
	uint32_t total_entry = 0;
	uint32_t capacity = 0;
	uint64_t seed = 0;
	uint32_t item = 0;
	struct {
		uint16_t r = 0;
		uint16_t w = 0;
	} recycle;
	struct {
		uint32_t head = 0;
		uint32_t tail = 0;
	} free_list;
};
using Header = LuckyEstuary::Meta;

uint32_t LuckyEstuary::item() const noexcept {
	return m_meta == nullptr? 0 : m_meta->item;
}

struct LuckyEstuary::Mutex {
	pthread_mutex_t core;
};
using Mutex = LuckyEstuary::Mutex;

static constexpr unsigned RECYCLE_CAPACITY = UINT16_MAX+1;
static constexpr unsigned RECYCLE_BIN_SIZE = UINT8_MAX+1;
static constexpr long RECYCLE_DELAY_MS = 50;

struct Node {
	static constexpr uint32_t END = UINT32_MAX;
	uint32_t next;
	union {
		uint8_t line[4];
		uint32_t free;
	};
};
#define ENTRY(key) (Hash((key), m_const.key_len, m_const.seed) % m_const.total_entry)
#define NODE(idx) ((Node*)(m_data+(idx)*(size_t)m_const.item_size))

//optimize for common short cases
static FORCE_INLINE bool Equal(const uint8_t* a, const uint8_t* b, uint8_t len) {
	if (len == sizeof(uint64_t)) {
		return *(const uint64_t*)a == *(const uint64_t*)b;
	} else {
		return memcmp(a, b, len) == 0;
	}
}

bool LuckyEstuary::fetch(const uint8_t* key, uint8_t* val) const {
	if (m_meta == nullptr || key == nullptr) {
		return false;
	}
	for (auto idx = m_table[ENTRY(key)]; idx != Node::END; ) {
		auto node = NODE(idx);
		if (Equal(node->line, key, m_const.key_len)) {
			memcpy(val, node->line+m_const.key_len, m_const.val_len);
			return true;
		}
		idx = node->next;
	}
	return false;
}

unsigned LuckyEstuary::batch_fetch(unsigned batch, const uint8_t* __restrict__ keys, uint8_t* __restrict__ data,
								   const uint8_t* __restrict__ dft_val) const {
	constexpr unsigned WINDOW_SIZE = 16;
	struct State {
		unsigned idx;
		uint32_t ent;
		Node* node;
	} states[WINDOW_SIZE];

	unsigned hit = 0;
	auto window = std::min(batch, WINDOW_SIZE);

	auto init_pipeline = [this, keys](State& state, unsigned idx) {
		state.idx = idx;
		state.node = nullptr;
		auto key = keys + idx * m_const.key_len;
		state.ent = Hash(key, m_const.key_len, m_const.seed) % m_const.total_entry;
		PrefetchForNext(&m_table[state.ent]);
	};

	unsigned idx = 0;
	for (; idx < window; idx++) {
		init_pipeline(states[idx], idx);
	}
	while (window > 0) {
		for (unsigned i = 0; i < window; ) {
			auto& cur = states[i];
			auto key = keys + cur.idx * m_const.key_len;
			auto out = data + cur.idx * m_const.val_len;
			uint32_t next = Node::END;
			if (cur.node == nullptr) {
				next = m_table[cur.ent];
			} else {
				if (Equal(key, cur.node->line, m_const.key_len)) {
					memcpy(out, cur.node->line+m_const.key_len, m_const.val_len);
					hit++;
					goto reload;
				} else {
					next = cur.node->next;
				}
			}
			if (next != Node::END) {
				cur.node = NODE(next);
				PrefetchForNext(cur.node);
				auto off = (uintptr_t)cur.node & (CACHE_BLOCK_SIZE-1);
				auto blk = (const void*)(((uintptr_t)cur.node & ~(uintptr_t)(CACHE_BLOCK_SIZE-1)) + CACHE_BLOCK_SIZE);
				if (off + sizeof(uint32_t)+m_const.key_len > CACHE_BLOCK_SIZE) {
					PrefetchForNext(blk);
				} else if (off + sizeof(uint32_t)+m_const.key_len+m_const.val_len > CACHE_BLOCK_SIZE) {
					PrefetchForFuture(blk);
				}
				i++;
				continue;
			} else if (dft_val != nullptr) {
				memcpy(out, dft_val, m_const.val_len);
			}
		reload:
			if (idx < batch) {
				init_pipeline(cur, idx++);
				i++;
			} else {
				cur = states[--window];
			}
		}
	}
	return hit;
}

bool LuckyEstuary::erase(const uint8_t* key) const {
	if (m_meta == nullptr || key == nullptr) {
		return false;
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

bool LuckyEstuary::_erase(const uint8_t* key) const {
	for (auto knot = (Node*)(&m_table[ENTRY(key)]); knot->next != Node::END;) {
		auto node = NODE(knot->next);
		if (Equal(node->line, key, m_const.key_len)) {
			auto vic = knot->next;
			knot->next = node->next;
			_recycle(vic);
			m_meta->item--;
			return true;
		}
		knot = node;
	}
	return false;
}

size_t LuckyEstuary::batch_update(IDataReader& source) const {
	auto total = source.total();
	if (m_meta == nullptr || total == 0) {
		return 0;
	}
	source.reset();
	MutexLock master_lock(&m_lock->core);
	if (m_meta->writing) {
		throw DataException();
	}
	m_meta->writing = true;
	size_t idx;
	for (idx = 0; idx < total; idx++) {
		auto rec = source.read();
		if (rec.key.ptr == nullptr || rec.key.len != m_const.key_len
			|| rec.val.len != m_const.val_len || (rec.val.len != 0 && rec.val.ptr == nullptr)
			|| !_update(rec.key.ptr, rec.val.ptr)) {
			break;
		}
	}
	m_meta->writing = false;
	return idx;
}

bool LuckyEstuary::update(const uint8_t* key, const uint8_t* val) const {
	if (m_meta == nullptr || key == nullptr || val == nullptr) {
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

static FORCE_INLINE int64_t GetStamp() {
	return std::chrono::time_point_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now()).time_since_epoch().count();
}

bool LuckyEstuary::_update(const uint8_t* key, const uint8_t* val) const {
	ConsistencyAssert(m_meta->free_list.head != Node::END);
	auto new_node = [this](const uint8_t* key, const uint8_t* val)->std::tuple<uint32_t,Node*> {
		auto id = m_meta->free_list.head;
		auto node = NODE(id);
		m_meta->free_list.head = node->free;
		if (node->free == Node::END) {
			m_meta->free_list.tail = Node::END;
		}
		if (m_const.key_len == sizeof(uint64_t)) {
			*(uint64_t*)node->line = *(const uint64_t*)key;
		} else {
			memcpy(node->line, key, m_const.key_len);
		}
		memcpy(node->line+m_const.key_len, val, m_const.val_len);
		return {id, node};
	};

	const auto entry = ENTRY(key);
	for (auto knot = (Node*)(&m_table[entry]); knot->next != Node::END;) {
		auto node = NODE(knot->next);
		if (Equal(node->line, key, m_const.key_len)) {
			if (LIKELY(memcmp(node->line+m_const.key_len, val, m_const.val_len) != 0)) {
				auto vic = knot->next;
				auto [id, neo] = new_node(key, val);
				neo->next = node->next;
				StoreRelease(knot->next, id);
				_recycle(vic);
			}
			return true;
		}
		knot = node;
	}
	if (m_meta->item >= m_const.capacity) {
		return false;
	}
	auto [id, neo] = new_node(key, val);
	neo->next = m_table[entry];
	StoreRelease(m_table[entry], id);
	m_meta->item++;
	return true;
}


static_assert(RECYCLE_DELAY_MS > 0);
static_assert(RECYCLE_BIN_SIZE < RECYCLE_CAPACITY && (RECYCLE_BIN_SIZE&(RECYCLE_BIN_SIZE-1)) == 0);

void LuckyEstuary::_recycle(uint32_t vic) const {
	static_assert(RECYCLE_CAPACITY == 1U<<(sizeof(m_meta->recycle.w)*8U)
		&& sizeof(m_meta->recycle.r) == sizeof(m_meta->recycle.w));
	assert(vic != Node::END);
	if ((m_meta->recycle.w+1)%RECYCLE_CAPACITY == m_meta->recycle.r) {	//full
		const auto stamp = m_stamps[m_meta->recycle.r/RECYCLE_BIN_SIZE];
		const auto now_ms = GetStamp();
		ConsistencyAssert(now_ms > stamp);
		const auto extra_delay = RECYCLE_DELAY_MS - (now_ms - stamp);
		if (extra_delay > 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(extra_delay));
		}
		ConsistencyAssert(m_meta->recycle.r % RECYCLE_BIN_SIZE == 0);
		const unsigned begin = m_meta->recycle.r;
		const unsigned end = begin + RECYCLE_BIN_SIZE;
		m_meta->recycle.r = end % RECYCLE_CAPACITY;
		Node fake;
		auto tail = &fake;
		for (unsigned i = begin; i < end; i++) {
			assert(m_recycle[i] != Node::END);
			tail->free = m_recycle[i];
			m_recycle[i] = Node::END;
			tail = NODE(tail->free);
			tail->next = Node::END;
		}
		tail->free = Node::END;
		if (m_meta->free_list.tail == Node::END) {
			assert(m_meta->free_list.head == Node::END);
			m_meta->free_list.head = fake.free;
		} else {
			NODE(m_meta->free_list.tail)->free = fake.free;
		}
		m_meta->free_list.tail = ((uint8_t*)tail - m_data) / m_const.item_size;
	}

	auto& stamp = m_stamps[m_meta->recycle.w/RECYCLE_BIN_SIZE];
	m_recycle[m_meta->recycle.w++] = vic;
	m_meta->recycle.w %= RECYCLE_CAPACITY;
	if (m_meta->recycle.w % RECYCLE_BIN_SIZE == 0) {
		stamp = GetStamp();
	}
}

static bool InitLocks(Mutex* lock, bool shared=true) {
	const int pshared = shared? PTHREAD_PROCESS_SHARED : PTHREAD_PROCESS_PRIVATE;
	pthread_mutexattr_t mutexattr;
	if (pthread_mutexattr_init(&mutexattr) != 0
		|| pthread_mutexattr_setpshared(&mutexattr, pshared) != 0
		|| pthread_mutex_init(&lock->core, &mutexattr) != 0) {
		//pthread_mutexattr_destroy is unnecessary
		return false;
	}
	return true;
}

LuckyEstuary::~LuckyEstuary() noexcept {
	if (m_meta == nullptr) {
		return;
	}
	if (m_monopoly_extra != nullptr) {
		pthread_mutex_destroy(&m_lock->core);
	}
}

static FORCE_INLINE size_t ItemSize(uint8_t key_len, uint32_t val_len) {
	return ((sizeof(uint32_t)+key_len+val_len)+(sizeof(uint32_t)-1)) & ~(sizeof(uint32_t)-1U);
}

LuckyEstuary LuckyEstuary::Load(const std::string& path, LoadPolicy policy) {
	LuckyEstuary out;
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
	if (!res || res.size() < sizeof(Meta)) {
		return out;
	}
	auto meta = (Meta*)res.addr();
	const auto lock_off = sizeof(Meta);
	const auto stamps_off = lock_off + sizeof(pthread_mutex_t);
	const auto recycle_off = stamps_off + sizeof(int64_t) * (RECYCLE_CAPACITY/RECYCLE_BIN_SIZE);
	const auto table_off = recycle_off + sizeof(uint32_t) * RECYCLE_CAPACITY;
	const auto data_off = table_off + sizeof(uint32_t) * meta->total_entry;
	const auto item_size = ItemSize(meta->key_len, meta->val_len);
	const auto capacity = meta->capacity + RECYCLE_CAPACITY;
	if (meta->magic != MAGIC || meta->key_len == 0 || meta->val_len > MAX_VAL_LEN
		|| meta->capacity < MIN_CAPACITY || meta->capacity > MAX_CAPACITY
		|| meta->total_entry == 0 || meta->capacity/meta->total_entry > MAX_LOAD_FACTOR
		|| res.size() < data_off + item_size * capacity) {
		Logger::Printf("broken file: %s\n", path.c_str());
		return out;
	}

	std::unique_ptr<uint8_t[]> monopoly_extra;
	auto lock = (Mutex*)(res.addr()+lock_off);
	if (policy != SHARED) {
		if (meta->writing) {
			Logger::Printf("file is not saved correctly: %s\n", path.c_str());
			return out;
		}
		monopoly_extra = std::make_unique<uint8_t[]>(sizeof(pthread_mutex_t));
		lock = (Mutex*)monopoly_extra.get();
		if (!InitLocks(lock)) {
			Logger::Printf("fail to reset locks in: %s\n", path.c_str());
			return out;
		}
	}

	assert(item_size >= sizeof(Node));

	out.m_meta = meta;
	out.m_lock = lock;
	out.m_stamps = (int64_t*)(res.addr()+stamps_off);
	out.m_recycle = (uint32_t*)(res.addr()+recycle_off);
	out.m_table = (uint32_t*)(res.addr()+table_off);
	out.m_data = res.addr()+data_off;
	out.m_monopoly_extra = std::move(monopoly_extra);
	out.m_resource = std::move(res);
	out.m_const.key_len = meta->key_len;
	out.m_const.val_len = meta->val_len;
	out.m_const.item_size = item_size;
	out.m_const.capacity = meta->capacity;
	out.m_const.seed = meta->seed;
	out.m_const.total_entry = meta->total_entry;
	return out;
}

bool LuckyEstuary::Create(const std::string& path, const Config& config, IDataReader* source) {
	if (config.capacity < MIN_CAPACITY || config.capacity > MAX_CAPACITY
		|| config.entry == 0 || config.capacity/config.entry > MAX_LOAD_FACTOR
		|| config.key_len == 0 || config.key_len > MAX_KEY_LEN || config.val_len > MAX_VAL_LEN) {
		Logger::Printf("bad arguments\n");
		return false;
	}
	Meta header;
	header.key_len = config.key_len;
	header.val_len = config.val_len;
	header.total_entry = config.entry;
	header.capacity = config.capacity;
	header.seed = GetSeed();

	static_assert(sizeof(Meta) % sizeof(uintptr_t) == 0, "alignment check");

	const auto item_size = ItemSize(header.key_len, header.val_len);
	const auto capacity = header.capacity + RECYCLE_CAPACITY;

	size_t size = sizeof(header);
	const auto lock_off = size;
	size += sizeof(pthread_mutex_t);
	size += sizeof(int64_t) * (RECYCLE_CAPACITY/RECYCLE_BIN_SIZE);
	const auto recycle_off = size;
	size += sizeof(uint32_t) * RECYCLE_CAPACITY;
	const auto table_off = size;
	size += sizeof(uint32_t) * header.total_entry;
	const auto data_off = size;
	size += item_size * capacity;

	MemMap res(path.c_str(), true, true, size);
	if (!res) {
		return false;
	}
	auto meta = (Meta*)res.addr();
	auto lock = (Mutex*)(res.addr() + lock_off);
	auto recycle = (uint32_t*)(res.addr()+recycle_off);
	auto table = (uint32_t*)(res.addr()+table_off);
	auto data = res.addr() + data_off;

	auto get_node = [data, item_size](uint32_t idx)->Node* {
		return (Node*)(data + idx*item_size);
	};

	*meta = header;
	if (!InitLocks(lock)) {
		Logger::Printf("fail to init\n");
		return false;
	}
	for (unsigned i = 0; i < RECYCLE_CAPACITY; i++) {
		recycle[i] = Node::END;
	}
	for (size_t i = 0; i < header.total_entry; i++) {
		table[i] = Node::END;
	}

	uint32_t cnt = 0;
	if (source != nullptr) {
		Divisor<uint64_t> total_entry(header.total_entry);
		source->reset();
		auto total = source->total();
		if (total > header.capacity) {
			Logger::Printf("too many items\n");
			return false;
		}
		for (size_t i = 0; i < total; i++) {
			auto rec = source->read();
			if (rec.key.ptr == nullptr || rec.key.len != header.key_len
				|| rec.val.len != header.val_len || (rec.val.len != 0 && rec.val.ptr == nullptr)) {
				Logger::Printf("broken item\n");
				return false;
			}
			bool found = false;
			const auto ent = Hash(rec.key.ptr, header.key_len, header.seed) % total_entry;
			for (auto idx = table[ent]; idx != Node::END; ) {
				auto node = get_node(idx);
				if (Equal(node->line, rec.key.ptr, header.key_len)) {
					found = true;
					memcpy(node->line+header.key_len, rec.val.ptr, header.val_len);
					break;
				}
				idx = node->next;
			}
			if (!found) {
				auto node = get_node(cnt);
				node->next = table[ent];
				table[ent] = cnt++;
				if (header.key_len == sizeof(uint64_t)) {
					*(uint64_t*)node->line = *(const uint64_t*)rec.key.ptr;
				} else {
					memcpy(node->line, rec.key.ptr, header.key_len);
				}
				memcpy(node->line+header.key_len, rec.val.ptr, header.val_len);
			}
		}
	}

	assert(cnt < capacity);
	meta->item = cnt;
	meta->free_list.head = cnt;
	meta->free_list.tail = capacity-1;
	while (cnt < capacity) {
		auto node = get_node(cnt);
		node->next = Node::END;
		node->free = ++cnt;
	}
	get_node(capacity-1)->free = Node::END;
	return true;
}

} //estuary