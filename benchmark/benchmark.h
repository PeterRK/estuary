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

#include <random>
#include "../test/test.h"

class XorShift128Plus final {
public:
	XorShift128Plus() {
		std::random_device rd;
		for (unsigned i = 0; i < 4; i++) {
			reinterpret_cast<uint32_t*>(_s)[i] = rd();
		}
	}
	uint64_t operator()() noexcept {
		uint64_t x = _s[0];
		const uint64_t y = _s[1];
		_s[0] = y;
		x ^= x << 23U;
		_s[1] = x ^ y ^ (x >> 17U) ^ (y >> 26U);
		return _s[1] + y;
	}
private:
	uint64_t _s[2];
};