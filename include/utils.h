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
#ifndef ESTUARY_UTILS_H
#define ESTUARY_UTILS_H

#include <cstdint>
#include <cstdarg>
#include <new>
#include <utility>
#include <type_traits>

namespace estuary {

class MemMap final {
public:
	MemMap() noexcept = default;
	~MemMap() noexcept;

	explicit MemMap(const char* path, bool populate=false, bool exclusive=false, size_t size=0) noexcept;

	struct LoadByCopy {};
	static constexpr LoadByCopy load_by_copy = {};
	explicit MemMap(const char* path, LoadByCopy);

	MemMap(MemMap&& other) noexcept
		: m_addr(other.m_addr), m_size(other.m_size), m_fd(other.m_fd) {
		other.m_addr = nullptr;
		other.m_size = 0;
		other.m_fd = -1;
	}
	MemMap& operator=(MemMap&& other) noexcept {
		if (&other != this) {
			this->~MemMap();
			new(this)MemMap(std::move(other));
		}
		return *this;
	}

	size_t size() const noexcept { return m_size; }
	uint8_t* addr() const noexcept { return m_addr; }
	const uint8_t* end() const noexcept { return m_addr + m_size; }
	bool operator!() const noexcept { return m_addr == nullptr; }
	bool dump(const char* path) const noexcept;
private:
	MemMap(const MemMap&) noexcept = delete;
	MemMap& operator=(const MemMap&) noexcept = delete;
	uint8_t* m_addr = nullptr;
	size_t m_size = 0;
	int m_fd = -1;
};

struct Slice final {
	const uint8_t* ptr = nullptr;
	size_t len = 0;
};

struct IDataReader {
	struct Record {
		Slice key;
		Slice val;
	};
	virtual void reset() = 0;
	virtual size_t total() = 0;
	virtual Record read() = 0;
	virtual ~IDataReader() noexcept = default;
};

class Logger {
public:
	virtual ~Logger() = default;
	virtual void printf(const char* format, va_list args) = 0;
	static void Printf(const char* format, ...);
	static Logger* Bind(Logger* logger) noexcept {
		auto old = s_instance;
		s_instance = logger;
		return old;
	}
private:
	static Logger* s_instance;
};

//Granlund-Montgomery
template <typename Word>
class Divisor final {
private:
	static_assert(std::is_same<Word,uint8_t>::value | std::is_same<Word,uint16_t>::value
				  || std::is_same<Word,uint32_t>::value || std::is_same<Word,uint64_t>::value, "");
	Word m_val = 0;
#ifndef DISABLE_SOFT_DIVIDE
	Word m_fac = 0;
	unsigned m_sft = 0;
	using DoubleWord = typename std::conditional<std::is_same<Word,uint8_t>::value, uint16_t,
		typename std::conditional<std::is_same<Word,uint16_t>::value, uint32_t,
			typename std::conditional<std::is_same<Word,uint32_t>::value, uint64_t, __uint128_t>::type>::type>::type;
	static constexpr unsigned BITWIDTH = sizeof(Word)*8;
#endif

public:
	Word value() const noexcept { return m_val; }
	Divisor() noexcept = default;
	explicit Divisor(Word n) noexcept { *this = n; }

	Divisor operator=(Word n) noexcept {
		m_val = n;
#ifndef DISABLE_SOFT_DIVIDE
		if (n == 0) {
			m_fac = 0;
			m_sft = 0;
		} else {
			m_sft = BITWIDTH - 1;
			constexpr Word one = 1;
			for (auto t = one << m_sft; t > n; t >>= 1U) {
				m_sft--;
			}
			constexpr DoubleWord dw_one = 1;
			const DoubleWord c = dw_one << (m_sft + BITWIDTH);
			m_fac = 2 * (c / n) + (2 * (c % n)) / n + 1 - (dw_one << BITWIDTH);
		}
#endif
		return *this;
	}

	Word div(Word m) const noexcept {
#ifdef DISABLE_SOFT_DIVIDE
		return m / m_val;
#else
		Word t = (m * (DoubleWord)m_fac) >> BITWIDTH;
		t += (m - t) >> 1U;
		if (m_fac <= 1U) {
			t = m;
		}
		return t >> m_sft;
#endif
	}

	 Word mod(Word m) const noexcept {
#ifdef DISABLE_SOFT_DIVIDE
		return m % m_val;
#else
		Word t = (m * (DoubleWord)m_fac) >> BITWIDTH;
		t += (m - t) >> 1U;
		auto out = m - m_val * (t >> m_sft);
		constexpr Word one = 1U;
		const auto tmp = m & ((one << m_sft) - 1U);
		if (m_fac <= 1U) {
			out = tmp;
		}
		return out;
#endif
	}
};

template <typename Word>
static inline Word operator/(Word m, const Divisor<Word>& d) noexcept {
	return d.div(m);
}

template <typename Word>
static inline Word operator%(Word m, const Divisor<Word>& d) noexcept {
	return d.mod(m);
}

} //estuary
#endif