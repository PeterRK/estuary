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
#include <vector>
#include <memory>
#include <gtest/gtest.h>
#include <lucky_estuary.h>
#include "test.h"

TEST(LuckyEstuary, BuildAndRead) {
	estuary::Logger::Bind(nullptr);
	const std::string filename = "tmp.les";
	constexpr unsigned PIECE = estuary::LuckyEstuary::MIN_CAPACITY+1;

	estuary::LuckyEstuary::Config config;
	config.entry = PIECE;
	config.key_len = sizeof(uint64_t);
	config.val_len = EmbeddingGenerator::VALUE_SIZE;

	EmbeddingGenerator source(0, PIECE);
	config.capacity = PIECE-1;
	ASSERT_FALSE(estuary::LuckyEstuary::Create(filename, config, &source));
	config.capacity = PIECE;
	ASSERT_TRUE(estuary::LuckyEstuary::Create(filename, config, &source));

	auto dict = estuary::LuckyEstuary::Load(filename);
	ASSERT_FALSE(!dict);
	ASSERT_EQ(dict.key_len(), config.key_len);
	ASSERT_EQ(dict.val_len(), config.val_len);
	ASSERT_EQ(dict.item(), PIECE);
	ASSERT_EQ(dict.capacity(), PIECE);

	std::vector<uint64_t> keys(PIECE*2);
	for (unsigned i = 0; i < PIECE; i++) {
		keys[2*i] = i;
		keys[2*i+1] = i+PIECE;
	}
	auto out = std::make_unique<uint8_t[]>(keys.size()*config.val_len);

	auto dft_val = std::make_unique<uint8_t[]>(EmbeddingGenerator::VALUE_SIZE);
	memset(dft_val.get(), 0x33, EmbeddingGenerator::VALUE_SIZE);

	ASSERT_EQ(dict.batch_fetch(PIECE*2, (const uint8_t*)keys.data(), out.get(), dft_val.get()), PIECE);

	EmbeddingGenerator check(0, PIECE*2);
	auto line = out.get();
	for (unsigned i = 0; i < PIECE; i++) {
		auto rec = check.read();
		ASSERT_EQ(memcmp(line, rec.val.ptr, rec.val.len), 0);
		line += EmbeddingGenerator::VALUE_SIZE;
		ASSERT_EQ(memcmp(line, dft_val.get(), rec.val.len), 0);
		ASSERT_TRUE(dict.fetch(rec.key.ptr, line));
		ASSERT_EQ(memcmp(line, rec.val.ptr, rec.val.len), 0);
		line += EmbeddingGenerator::VALUE_SIZE;
	}

	line = out.get();
	for (unsigned i = 0; i < PIECE; i++) {
		auto rec = check.read();
		ASSERT_FALSE(dict.fetch(rec.key.ptr, line));
	}
}

TEST(LuckyEstuary, Write) {
	const std::string filename = "test.les";
	constexpr unsigned PIECE = estuary::LuckyEstuary::MIN_CAPACITY;

	estuary::LuckyEstuary::Config config;
	config.entry = PIECE * 6 / 5;
	config.capacity = PIECE*2;
	config.key_len = sizeof(uint64_t);
	config.val_len = EmbeddingGenerator::VALUE_SIZE;

	EmbeddingGenerator input1(0, PIECE, EmbeddingGenerator::MASK0);
	ASSERT_TRUE(estuary::LuckyEstuary::Create(filename, config, &input1));

	auto dict = estuary::LuckyEstuary::Load(filename);
	ASSERT_FALSE(!dict);

	EmbeddingGenerator input2(0, PIECE*2+1, EmbeddingGenerator::MASK1);
	for (unsigned i = 0; i < PIECE; i++) {
		auto rec = input2.read();
		ASSERT_TRUE(dict.update(rec.key.ptr, rec.val.ptr));
	}
	for (unsigned i = 0; i < PIECE; i++) {
		auto rec = input2.read();
		ASSERT_TRUE(dict.update(rec.key.ptr, rec.val.ptr));
	}

	std::vector<uint64_t> keys(PIECE*2);
	for (unsigned i = 0; i < PIECE*2; i++) {
		keys[i] = i;
	}
	auto out = std::make_unique<uint8_t[]>(keys.size()*config.val_len);

	ASSERT_EQ(dict.batch_fetch(PIECE, (const uint8_t*)keys.data(), out.get()), PIECE);

	input2.reset();
	auto line = out.get();
	for (unsigned i = 0; i < PIECE; i++) {
		auto rec = input2.read();
		ASSERT_EQ(memcmp(line, rec.val.ptr, rec.val.len), 0);
		line += EmbeddingGenerator::VALUE_SIZE;
	}

	ASSERT_EQ(dict.batch_update(input1), PIECE);

	ASSERT_EQ(dict.batch_fetch(PIECE*2, (const uint8_t*)keys.data(), out.get()), PIECE*2);

	input1.reset();
	line = out.get();
	for (unsigned i = 0; i < PIECE; i++) {
		auto rec = input1.read();
		ASSERT_EQ(memcmp(line, rec.val.ptr, rec.val.len), 0);
		line += EmbeddingGenerator::VALUE_SIZE;
	}
	for (unsigned i = 0; i < PIECE; i++) {
		auto rec = input2.read();
		ASSERT_EQ(memcmp(line, rec.val.ptr, rec.val.len), 0);
		line += EmbeddingGenerator::VALUE_SIZE;
	}

	auto rec = input2.read();
	ASSERT_FALSE(dict.update(rec.key.ptr, rec.val.ptr));
	ASSERT_FALSE(dict.fetch(rec.key.ptr, out.get()));
	ASSERT_FALSE(dict.erase(rec.key.ptr));
	ASSERT_TRUE(dict.erase((const uint8_t*)keys.data()));
	ASSERT_FALSE(dict.fetch((const uint8_t*)keys.data(), out.get()));
	ASSERT_TRUE(dict.update(rec.key.ptr, rec.val.ptr));
	ASSERT_TRUE(dict.fetch(rec.key.ptr, out.get()));
	ASSERT_EQ(memcmp(out.get(), rec.val.ptr, rec.val.len), 0);
}
