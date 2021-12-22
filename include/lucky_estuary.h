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
#ifndef LUCKY_ESTUARY_H
#define LUCKY_ESTUARY_H

#include <cstddef>
#include <cstdint>
#include <new>
#include <string>
#include <memory>
#include "utils.h"

namespace estuary {

class LuckyEstuary final {
public:
	bool fetch(const uint8_t* key, uint8_t* val) const;
	unsigned batch_fetch(unsigned batch, const uint8_t* __restrict__ keys, uint8_t* __restrict__ data,
						 const uint8_t* __restrict__ dft_val=nullptr) const;
	bool erase(const uint8_t* key) const;
	bool update(const uint8_t* key, const uint8_t* val) const;
	size_t batch_update(IDataReader& source) const;

	bool operator!() const noexcept { return m_meta == nullptr; }
	unsigned key_len() const noexcept { return m_const.key_len; }
	unsigned val_len() const noexcept { return m_const.val_len; }
	uint32_t capacity() const noexcept { return m_const.capacity; }
	uint32_t item() const noexcept;

	LuckyEstuary() = default;
	LuckyEstuary(LuckyEstuary&& other) noexcept
		: m_resource(std::move(other.m_resource)), m_meta(other.m_meta), m_const(other.m_const),
		  m_lock(other.m_lock), m_stamps(other.m_stamps), m_recycle(other.m_recycle), m_table(other.m_table),
		  m_data(other.m_data), m_monopoly_extra(std::move(other.m_monopoly_extra))
	{
		other.m_meta = nullptr;
		other.m_lock = nullptr;
		other.m_stamps = nullptr;
		other.m_recycle = nullptr;
		other.m_table = nullptr;
		other.m_data = nullptr;
	}
	LuckyEstuary& operator=(LuckyEstuary&& other) noexcept {
		if (&other != this) {
			this->~LuckyEstuary();
			new(this)LuckyEstuary(std::move(other));
		}
		return *this;
	}
	~LuckyEstuary() noexcept;

	static constexpr unsigned MAX_KEY_LEN = UINT8_MAX;
	static constexpr unsigned MAX_VAL_LEN = UINT16_MAX+1;
	static constexpr size_t MIN_CAPACITY = UINT16_MAX+1;
	static constexpr size_t MAX_CAPACITY = UINT32_MAX-(UINT16_MAX+1);
	static constexpr size_t MAX_LOAD_FACTOR = 2;
	struct Config {
		uint32_t entry = MIN_CAPACITY;
		uint32_t capacity = MIN_CAPACITY;
		unsigned key_len = sizeof(uint64_t);		//1-255
		unsigned val_len = 0;						//0-65536
	};

	static bool Create(const std::string& path, const Config& config, IDataReader* source=nullptr);
	enum LoadPolicy {SHARED, MONOPOLY, COPY_DATA};
	static LuckyEstuary Load(const std::string& path, LoadPolicy policy=MONOPOLY);
  static LuckyEstuary Load(size_t size, const std::function<bool(uint8_t*)>& load);

	// only capacity can be extended, entry cannot
	// percent should be 1-100
	static bool Extend(const std::string& path, unsigned percent, Config* result=nullptr);

	bool dump(const std::string& path) const noexcept {
		return m_resource.dump(path.c_str());
	}

	struct Meta;
	struct Lock;

private:
	MemMap m_resource;
	Meta* m_meta = nullptr;
	struct {
		uint8_t key_len = 0;
		uint32_t val_len = 0;
		uint32_t item_size = 0;
		uint32_t capacity = 0;
		uint64_t seed = 0;
		Divisor<uint64_t> total_entry;
	} m_const;
	Lock* m_lock = nullptr;
	int64_t* m_stamps = nullptr;
	uint32_t* m_recycle = nullptr;
	uint32_t* m_table = nullptr;
	uint8_t* m_data = nullptr;
	std::unique_ptr<uint8_t[]> m_monopoly_extra;

	void _recycle(uint32_t vic) const;
	bool _erase(const uint8_t* key) const;
	bool _update(const uint8_t* key, const uint8_t* val) const;

  void _init(MemMap&& res, bool monopoly, const char* path);
};

} //estuary
#endif