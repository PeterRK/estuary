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

#include <limits>
#include <random>
#include <gtest/gtest.h>
#include <utils.h>


template<typename Word>
void DoTestDivisor(Word n) {
	ASSERT_NE(n, 0);
	estuary::Divisor<Word> d(n);
	std::mt19937_64 rand;

	auto test = [&d](Word m) {
		ASSERT_EQ(m / d, m / d.value());
		ASSERT_EQ(m % d, m % d.value());
	};
	test(0);
	test(1);
	test(std::numeric_limits<Word>::max());

	for (unsigned i = 0; i < 1000; i++) {
		Word m = rand();
		test(m);
	}
}

template<typename Word>
void TestDivisor() {
	DoTestDivisor<Word>(std::numeric_limits<Word>::max());
	DoTestDivisor<Word>(std::numeric_limits<Word>::max()/2+1);
	DoTestDivisor<Word>(std::numeric_limits<Word>::max()/2);
	DoTestDivisor<Word>(17);
	DoTestDivisor<Word>(13);
	DoTestDivisor<Word>(11);
	DoTestDivisor<Word>(9);
	DoTestDivisor<Word>(7);
	DoTestDivisor<Word>(5);
	DoTestDivisor<Word>(3);
	DoTestDivisor<Word>(2);
	DoTestDivisor<Word>(1);
}

TEST(Divisor, Uint64) {
	TestDivisor<uint64_t>();
}

TEST(Divisor, Uint32) {
	TestDivisor<uint32_t>();
}

TEST(Divisor, Uint16) {
	TestDivisor<uint16_t>();
}

TEST(Divisor, Uint8) {
	TestDivisor<uint8_t>();
}


