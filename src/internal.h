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

#pragma once
#ifndef ESTUARY_INTERNAL_H
#define ESTUARY_INTERNAL_H

#include <cstdint>
#include <chrono>
#include <exception>
#include <pthread.h>

#define FORCE_INLINE inline __attribute__((always_inline))
#define NOINLINE __attribute__((noinline))

#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
#error "little endian only"
#endif
static_assert(sizeof(uintptr_t)==sizeof(uint64_t));

#define LIKELY(exp) __builtin_expect((exp),1)
#define UNLIKELY(exp) __builtin_expect((exp),0)

namespace estuary {
extern uint64_t Hash(const uint8_t* msg, uint8_t len, uint64_t seed) noexcept;

struct LockException : public std::exception {
	const char* what() const noexcept override;
};
struct DataException : public std::exception {
	const char* what() const noexcept override;
};

static FORCE_INLINE void ConsistencyAssert(bool condition) {
#ifdef ENABLE_CONSISTENCY_CHECK
	if (UNLIKELY(!condition)) {
		throw DataException();
	}
#endif
}

template <typename T>
class LockGuard final {
public:
	explicit LockGuard(typename T::LockType* lock) : m_lock(lock) {
		assert(m_lock != nullptr);
		if (UNLIKELY(T::Lock(m_lock) != 0)) {
			throw LockException();
		}
	};
	~LockGuard() noexcept {
		T::Unlock(m_lock);
	}
	LockGuard(LockGuard&& other) : m_lock(other.m_lock) {
		other.m_lock = nullptr;
	}
	LockGuard& operator=(LockGuard&& other) noexcept {
		if (&other != this) {
			this->~LockGuard();
			this->m_lock = other.m_lock;
			other.m_lock = nullptr;
		}
		return *this;
	}

private:
	typename T::LockType* m_lock = nullptr;
	LockGuard(const LockGuard&) noexcept = delete;
	LockGuard& operator=(const LockGuard&) noexcept = delete;
};

struct _MutexLock {
	using LockType = pthread_mutex_t;
	static constexpr auto Lock = pthread_mutex_lock;
	static constexpr auto Unlock = pthread_mutex_unlock;
};
using MutexLock = LockGuard<_MutexLock>;

static uint64_t GetSeed() {
	//return 1596176575357415943ULL;
	return std::chrono::duration_cast<std::chrono::nanoseconds>(
			std::chrono::system_clock::now().time_since_epoch()).count();
}

static FORCE_INLINE void PrefetchForNext(const void* ptr) {
	__builtin_prefetch(ptr, 0, 3);
}
static FORCE_INLINE void PrefetchForFuture(const void* ptr) {
	__builtin_prefetch(ptr, 0, 0);
}

#ifndef CACHE_BLOCK_SIZE
#define CACHE_BLOCK_SIZE 64U
#endif
static_assert(CACHE_BLOCK_SIZE >= 64U && (CACHE_BLOCK_SIZE&(CACHE_BLOCK_SIZE-1)) == 0);

template <typename T>
T FORCE_INLINE LoadRelaxed(const T& tgt) {
	return __atomic_load_n(&tgt, __ATOMIC_RELAXED);
}

template <typename T>
void FORCE_INLINE StoreRelease(T& tgt, T val) {
	__atomic_store_n(&tgt, val, __ATOMIC_RELEASE);
}

template <typename T>
T FORCE_INLINE AddRelaxed(T& tgt, T val) {
	return __atomic_fetch_add(&tgt, val, __ATOMIC_RELAXED);
}

template <typename T>
T FORCE_INLINE SubRelaxed(T& tgt, T val) {
	return __atomic_fetch_sub(&tgt, val, __ATOMIC_RELAXED);
}

void FORCE_INLINE MemoryBarrier() {
	__atomic_thread_fence(__ATOMIC_SEQ_CST);
}

} //estuary
#endif //ESTUARY_INTERNAL_H