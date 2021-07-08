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
#ifndef ESTUARY_H
#define ESTUARY_H

#include <cstdarg>
#include <cstdint>
#include <cstring>
#include <new>
#include <string>
#include <vector>
#include <memory>
#include <exception>
#include "utils.h"

namespace estuary {

class Estuary final {
public:
	bool fetch(Slice key, std::string& out) const;
	bool erase(Slice key) const;
	bool update(Slice key, Slice val) const;

	bool operator!() const noexcept { return m_meta == nullptr; }
	unsigned max_key_len() const noexcept { return m_const.max_key_len; }
	unsigned max_val_len() const noexcept { return m_const.max_val_len; }
	size_t item() const noexcept;
	size_t data_free() const;
	size_t item_limit() const;

	Estuary() = default;
	Estuary(Estuary&& other) noexcept
		: m_resource(std::move(other.m_resource)), m_const(other.m_const),
		  m_meta(other.m_meta), m_locks(other.m_locks), m_table(other.m_table), m_data(other.m_data),
		  m_monopoly_extra(std::move(other.m_monopoly_extra)) {
		other.m_meta = nullptr;
		other.m_locks = nullptr;
		other.m_table = nullptr;
		other.m_data = nullptr;
	}
	Estuary& operator=(Estuary&& other) noexcept {
		if (&other != this) {
			this->~Estuary();
			new(this)Estuary(std::move(other));
		}
		return *this;
	}
	~Estuary() noexcept;

	static constexpr unsigned MAX_KEY_LEN = UINT8_MAX;
	static constexpr unsigned MAX_VAL_LEN = (1U<<24U)-1U;
	struct Config {
		size_t item_limit = 1000;			//128-8589934656
		unsigned max_key_len = 32;			//1-255
		unsigned max_val_len = 1048576;		//1-16777215
		unsigned avg_size_per_item = 2048;	//2-16777215
		unsigned concurrency = 64;			//1-512
	};

	static bool Create(const std::string& path, const Config& config, IDataReader* source=nullptr);
	static bool ResetLocks(const std::string& path);

	enum LoadPolicy {SHARED, MONOPOLY, COPY_DATA};
	//concurrency > 0 means overwriting the origin value in monopoly mode
	static Estuary Load(const std::string& path, LoadPolicy policy=MONOPOLY, unsigned concurrency=0);

	bool dump(const std::string& path) const noexcept {
		return m_resource.dump(path.c_str());
	}

	struct Meta;
	struct Locks;

private:
	MemMap m_resource;
	Meta* m_meta = nullptr;
	struct {
		uint16_t lock_mask = 0;
		uint8_t max_key_len = 0;
		uint32_t max_val_len = 0;
		uint32_t seed = 0;
		uint32_t reserved_block = 0;
		size_t total_block = 0;
		Divisor<uint64_t> total_entry;
	} m_const;
	Locks* m_locks = nullptr;
	uint64_t* m_table = nullptr;
	uint8_t* m_data = nullptr;
	std::unique_ptr<uint8_t[]> m_monopoly_extra;

	Estuary(const Estuary&) noexcept = delete;
	Estuary& operator=(const Estuary&) noexcept = delete;

	bool _fetch(Slice key, std::string& out) const;
	bool _erase(Slice key) const;
	bool _update(Slice key, Slice val) const;
};

} //estuary
#endif //ESTUARY_H