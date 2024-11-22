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

#include <string>
#include <gtest/gtest.h>
#include <estuary.h>
#include "test.h"

static constexpr unsigned PIECE = 1000;

const estuary::Estuary::Config CONFIG = {
	.item_limit = PIECE,
	.max_key_len = sizeof(uint64_t),
	.max_val_len = UINT8_MAX,
	.avg_item_size = UINT8_MAX / 2 + 1 + sizeof(uint64_t)
};

TEST(Estuary, BuildAndRead) {
	estuary::Logger::Bind(nullptr);
	const std::string filename = "tmp.es";

	VariedValueGenerator source(0, PIECE);
	ASSERT_TRUE(estuary::Estuary::Create(filename, CONFIG, &source));

	auto dict = estuary::Estuary::Load(filename);
	ASSERT_FALSE(!dict);
	ASSERT_EQ(dict.max_key_len(), CONFIG.max_key_len);
	ASSERT_EQ(dict.max_val_len(), CONFIG.max_val_len);
	ASSERT_EQ(dict.item(), PIECE);

	std::string val;
	source.reset();
	for (unsigned i = 0; i < PIECE; i++) {
		auto rec = source.read();
		ASSERT_TRUE(dict.fetch(rec.key, val));
		ASSERT_EQ(val.size(), rec.val.len);
		ASSERT_EQ(memcmp(val.data(), rec.val.ptr, rec.val.len), 0);
	}
	uint8_t junk_key[8] = {0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff};
	ASSERT_FALSE(dict.fetch({junk_key,8}, val));
}

TEST(Estuary, Update) {
	estuary::Logger::Bind(nullptr);
	const std::string filename = "update.es";

	VariedValueGenerator input1(0, PIECE, 5);
	ASSERT_TRUE(estuary::Estuary::Create(filename, CONFIG, &input1));

	estuary::Estuary::Config ext_cfg;
	ASSERT_TRUE(estuary::Estuary::Extend(filename, 1, &ext_cfg));
	ASSERT_EQ(ext_cfg.item_limit, CONFIG.item_limit);
	ASSERT_GT(ext_cfg.avg_item_size, CONFIG.avg_item_size);

	auto dict = estuary::Estuary::Load(filename);
	ASSERT_FALSE(!dict);

	std::string val;
	input1.reset();
	for (unsigned i = 0; i < PIECE; i++) {
		auto rec = input1.read();
		if (i % 2 != 0) {
			ASSERT_TRUE(dict.erase(rec.key));
		}
	}

	VariedValueGenerator input2(1, PIECE, 10);
	for (unsigned i = 1; i < PIECE; i++) {
		auto rec = input2.read();
		ASSERT_TRUE(dict.update(rec.key, rec.val));
	}

	input1.reset();
	auto rec = input1.read();
	ASSERT_TRUE(dict.fetch(rec.key, val));
	ASSERT_EQ(val.size(), rec.val.len);
	ASSERT_EQ(memcmp(val.data(), rec.val.ptr, rec.val.len), 0);

	input2.reset();
	for (unsigned i = 1; i < PIECE; i++) {
		rec = input2.read();
		ASSERT_TRUE(dict.fetch(rec.key, val));
		ASSERT_EQ(val.size(), rec.val.len);
		ASSERT_EQ(memcmp(val.data(), rec.val.ptr, rec.val.len), 0);
		rec = input1.read();
		ASSERT_NE(val.size(), rec.val.len);
		ASSERT_TRUE(dict.update(rec.key, rec.val));
	}

	input1.reset();
	for (unsigned i = 0; i < PIECE; i++) {
		rec = input1.read();
		ASSERT_TRUE(dict.fetch(rec.key, val));
		ASSERT_EQ(val.size(), rec.val.len);
		ASSERT_EQ(memcmp(val.data(), rec.val.ptr, rec.val.len), 0);
	}
}

TEST(Estuary, Erase) {
	estuary::Logger::Bind(nullptr);
	const std::string filename = "erase.es";

	ASSERT_TRUE(estuary::Estuary::Create(filename, CONFIG));

	auto dict = estuary::Estuary::Load(filename);
	ASSERT_FALSE(!dict);

	VariedValueGenerator input1(0, PIECE*4, 5);
	VariedValueGenerator input2(0, PIECE*3, 10);

	for (unsigned k = 0; k < 3; k++) {
		for (unsigned i = 0; i < PIECE; i++) {
			auto rec = input1.read();
			ASSERT_TRUE(dict.update(rec.key, rec.val));
		}
		for (unsigned i = 0; i < PIECE; i++) {
			auto rec = input2.read();
			ASSERT_TRUE(dict.erase(rec.key));
		}
	}
	for (unsigned i = 0; i < PIECE; i++) {
		auto rec = input1.read();
		ASSERT_TRUE(dict.update(rec.key, rec.val));
	}
	input2.reset();
	std::string val;
	for (unsigned i = 0; i < PIECE*3; i++) {
		auto rec = input2.read();
		ASSERT_FALSE(dict.fetch(rec.key, val));
	}

	VariedValueGenerator input3(PIECE * 3, PIECE * 4, 5);
	for (unsigned i = 0; i < PIECE; i++) {
		auto rec = input3.read();
		ASSERT_TRUE(dict.fetch(rec.key, val));
		ASSERT_EQ(val.size(), rec.val.len);
		ASSERT_EQ(memcmp(val.data(), rec.val.ptr, rec.val.len), 0);
	}

	input1.reset();
	input2.reset();
	input3.reset();
	for (unsigned i = 0; i < PIECE/2; i++) {
		auto rec = input3.read();
		ASSERT_TRUE(dict.erase(rec.key));
	}
	for (unsigned i = 0; i < PIECE/2; i++) {
		auto rec = input1.read();
		ASSERT_TRUE(dict.update(rec.key, rec.val));
	}
	for (unsigned i = PIECE/2; i < PIECE; i++) {
		auto rec = input3.read();
		ASSERT_TRUE(dict.erase(rec.key));
	}
	for (unsigned i = 0; i < PIECE/2; i++) {
		auto rec = input2.read();
		ASSERT_TRUE(dict.update(rec.key, rec.val));
	}
	for (unsigned i = PIECE/2; i < PIECE; i++) {
		auto rec = input1.read();
		ASSERT_TRUE(dict.update(rec.key, rec.val));
	}
	input1.reset();
	input2.reset();
	for (unsigned i = 0; i < PIECE/2; i++) {
		auto rec = input2.read();
		ASSERT_TRUE(dict.fetch(rec.key, val));
		ASSERT_EQ(val.size(), rec.val.len);
		ASSERT_EQ(memcmp(val.data(), rec.val.ptr, rec.val.len), 0);
		ASSERT_TRUE(dict.erase(rec.key));
	}
	for (unsigned i = 0; i < PIECE/2; i++) {
		auto rec = input1.read();
		ASSERT_FALSE(dict.fetch(rec.key, val));
	}
	for (unsigned i = PIECE/2; i < PIECE; i++) {
		auto rec = input1.read();
		ASSERT_TRUE(dict.fetch(rec.key, val));
		ASSERT_EQ(val.size(), rec.val.len);
		ASSERT_EQ(memcmp(val.data(), rec.val.ptr, rec.val.len), 0);
	}
}