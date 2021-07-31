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
#include <sched.h>
#include "spin_rwlock.h"

#if defined(__amd64__) || defined(__i386__)
#define PAUSE __asm__ volatile ("pause");
#elif defined(__aarch64__) || defined(__arm__)
#define PAUSE __asm__ volatile ("yield");
#endif

namespace estuary {

class NanoSleeper {
public:
	void sleep() noexcept {
#ifdef PAUSE
		if (m_cnt <= 16) {
			for (unsigned i = 0; i < m_cnt; i++) {
				PAUSE
			}
			m_cnt *= 2;
			return;
		}
#endif
		sched_yield();
		reset();
	}
	void reset() noexcept {
		m_cnt = 1;
	}
private:
	unsigned m_cnt = 1;
};

static_assert(sizeof(SpinRWLock::state_t) >= sizeof(uint16_t) && sizeof(SpinRWLock::state_t) <= sizeof(uintptr_t));

static constexpr SpinRWLock::state_t ONE = 1;
static constexpr unsigned BIT_WIDTH = sizeof(SpinRWLock::state_t)*8;
static constexpr SpinRWLock::state_t WRITING = ONE << (BIT_WIDTH-1);
static constexpr SpinRWLock::state_t WAIT_TO_WRITE = ONE << (BIT_WIDTH-2);
static constexpr SpinRWLock::state_t READ_GUARD = ONE << (BIT_WIDTH-3);

void SpinRWLock::read_lock() noexcept {
	NanoSleeper sleeper;
	while (true) {
		constexpr state_t mask = WRITING | WAIT_TO_WRITE | READ_GUARD;
		auto state = __atomic_load_n(&m_state, __ATOMIC_RELAXED);
		if ((state & mask) == 0) {
			state = __atomic_fetch_add(&m_state, 1, __ATOMIC_ACQ_REL);
			if ((state & mask) == 0) {
				return;
			}
			__atomic_fetch_sub(&m_state, 1, __ATOMIC_RELAXED);
		}
		sleeper.sleep();
	}
}

void SpinRWLock::read_unlock() noexcept {
	auto state = __atomic_fetch_sub(&m_state, 1, __ATOMIC_RELEASE);
	assert((state & (state_t)~(WRITING | WAIT_TO_WRITE)) != 0);
}

void SpinRWLock::write_lock() noexcept {
	NanoSleeper sleeper;
	while (true) {
		auto state = __atomic_load_n(&m_state, __ATOMIC_RELAXED);
		if ((state & (state_t)~WAIT_TO_WRITE) == 0) {
			if (__atomic_compare_exchange_n(&m_state, &state, WRITING, false, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) {
				return;
			}
			sleeper.reset();
		} else if ((state & (state_t)(WRITING | WAIT_TO_WRITE)) == 0) {
			__atomic_fetch_or(&m_state, WAIT_TO_WRITE, __ATOMIC_RELAXED);
		}
		sleeper.sleep();
	}
}

void SpinRWLock::write_unlock() noexcept {
	auto state = __atomic_fetch_and(&m_state, (state_t)~(WRITING | WAIT_TO_WRITE), __ATOMIC_RELEASE);
	assert((state & WRITING) != 0);
}

} //estuary