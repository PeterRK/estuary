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
#ifdef __linux__
#include <cstdio>
#include <cstring>
#endif
#include <random>
#include <gtest/gtest.h>
#include <utils.h>

#ifdef __linux__
namespace estuary {
unsigned GetHugePageShift() noexcept;
}

static size_t ReadHugePageSize() {
	auto file = fopen("/proc/meminfo", "r");
	if (file == nullptr) {
		return 0;
	}
	size_t size = 0;
	char line[256];
	while (fgets(line, sizeof(line), file) != nullptr) {
		static constexpr char prefix[] = "Hugepagesize:";
		if (strncmp(line, prefix, sizeof(prefix)-1) != 0) {
			continue;
		}
		unsigned long long size_kb = 0;
		char unit[3] = {};
		if (sscanf(line + sizeof(prefix)-1, " %llu %2s", &size_kb, unit) == 2
				&& strcmp(unit, "kB") == 0
				&& size_kb <= std::numeric_limits<size_t>::max()/1024) {
			size = static_cast<size_t>(size_kb) * 1024;
		}
		break;
	}
	fclose(file);
	return size;
}

TEST(MemMap, DetectHugePageShiftAtStartup) {
	const auto size = ReadHugePageSize();
	const auto shift = estuary::GetHugePageShift();
	if (size == 0 || size > 16*1024*1024 || (size & (size-1)) != 0) {
		ASSERT_EQ(shift, 0);
	} else {
		ASSERT_LT(shift, std::numeric_limits<size_t>::digits);
		ASSERT_EQ(size_t{1} << shift, size);
	}
}
#endif


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
