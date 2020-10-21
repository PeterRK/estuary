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

#include <cstring>
#include <utils.h>

class EmbeddingGenerator : public estuary::IDataReader {
public:
	static constexpr uint64_t MASK0 = 0xaaaaaaaaaaaaaaaaUL;
	static constexpr uint64_t MASK1 = 0x5555555555555555UL;
	explicit EmbeddingGenerator(uint64_t begin, uint64_t total, uint64_t mask=MASK0)
			: m_current(begin-1), m_begin(begin), m_total(total), m_mask(mask)
	{}
	EmbeddingGenerator(const EmbeddingGenerator&) = delete;
	EmbeddingGenerator& operator=(const EmbeddingGenerator&) = delete;

	void reset() override {
		m_current = m_begin-1;
	}
	size_t total() override {
		return m_total;
	}
	estuary::IDataReader::Record read() override {
		m_current++;
		auto arr = (uint64_t*)m_val;
		arr[0] = m_current ^ m_mask;
		arr[1] = m_current ^ m_mask;
		arr[2] = m_current ^ m_mask;
		arr[3] = m_current ^ m_mask;
		return {{(const uint8_t*)&m_current, sizeof(uint64_t)}, {m_val, VALUE_SIZE}};
	}
	static constexpr unsigned VALUE_SIZE = 32;	//fp16 * 16

private:
	uint64_t m_current;
	uint8_t m_val[VALUE_SIZE];
	const uint64_t m_begin;
	const uint64_t m_total;
	const uint64_t m_mask;
};

class VariedValueGenerator : public estuary::IDataReader {
public:
	explicit VariedValueGenerator(uint64_t begin, uint64_t total, unsigned shift=5U)
		: m_current(begin-1), m_begin(begin), m_total(total), m_shift(shift)
	{}
	VariedValueGenerator(const VariedValueGenerator&) = delete;
	VariedValueGenerator& operator=(const VariedValueGenerator&) = delete;

	void reset() override {
		m_current = m_begin-1;
	}
	size_t total() override {
		return m_total;
	}
	estuary::IDataReader::Record read() override {
		m_current++;
		const uint8_t len = m_current + m_shift;
		memset(m_val, len, len);
		return {{(const uint8_t*)&m_current, sizeof(uint64_t)}, {m_val, len}};
	}

private:
	uint64_t m_current;
	uint8_t m_val[UINT8_MAX];
	const uint64_t m_begin;
	const uint64_t m_total;
	const unsigned m_shift;
};

