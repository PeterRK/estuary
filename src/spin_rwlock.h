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
#ifndef ESTUARY_SPIN_RWLOCK_H
#define ESTUARY_SPIN_RWLOCK_H

#include <cstdint>

namespace estuary {

class SpinRWLock {
public:
	SpinRWLock() = default;
	SpinRWLock(const SpinRWLock&) = delete;
	SpinRWLock(SpinRWLock&&) = delete;
	SpinRWLock& operator=(const SpinRWLock&) = delete;
	SpinRWLock& operator=(SpinRWLock&&) = delete;

	void init() noexcept {
		m_state = 0;
	}

	void read_lock() noexcept;
	void read_unlock() noexcept;
	void write_lock() noexcept;
	void write_unlock() noexcept;

	//using state_t = uintptr_t;
	using state_t = uint16_t;

private:
	state_t m_state = 0;
};

} //estuary
#endif //ESTUARY_SPIN_RWLOCK_H