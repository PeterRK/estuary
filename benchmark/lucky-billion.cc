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

#include <cstring>
#include <iostream>
#include <algorithm>
#include <thread>
#include <memory>
#include <vector>
#include <string>
#include <chrono>
#include <sys/sysinfo.h>
#include <lucky_estuary.h>
#include <gflags/gflags.h>
#include "benchmark.h"

DEFINE_string(file, "bench.les", "dict filename");
DEFINE_uint32(thread, 4, "number of worker threads");
DEFINE_bool(build, false, "build instead of fetching");
DEFINE_bool(copy, false, "load by copy");
DEFINE_bool(disable_write, false, "disable write");

static constexpr size_t BILLION = 1UL << 30U;

class RandEmbGenerator : public estuary::IDataReader {
public:
	explicit RandEmbGenerator(uint64_t total, uint64_t range)
		: m_total(total), m_range(range)
	{
		_reset();
	}
	RandEmbGenerator(const RandEmbGenerator&) = delete;
	RandEmbGenerator& operator=(const RandEmbGenerator&) = delete;

	void reset() override {
		_reset();
	}
	size_t total() override {
		return m_total;
	}
	estuary::IDataReader::Record read() override {
		m_key = m_rand() % m_range;
		return {{(const uint8_t*)&m_key, sizeof(uint64_t)}, {m_val, VALUE_SIZE}};
	}
	static constexpr unsigned VALUE_SIZE = 32;	//fp16 * 16
	static_assert(VALUE_SIZE == EmbeddingGenerator::VALUE_SIZE);

private:
	const uint64_t m_total;
	const uint64_t m_range;
	XorShift128Plus m_rand;
	uint64_t m_key;
	uint8_t m_val[VALUE_SIZE];

	void _reset() {
		auto arr = (uint64_t*)m_val;
		arr[0] = m_rand();
		arr[1] = m_rand();
		arr[2] = m_rand();
		arr[3] = m_rand();
	}
};

static int BenchFetch() {
	auto mode = FLAGS_copy? estuary::LuckyEstuary::COPY_DATA : estuary::LuckyEstuary::MONOPOLY;
	auto dict = estuary::LuckyEstuary::Load(FLAGS_file, mode);
	if (!dict) {
		std::cout << "fail to load: " << FLAGS_file << std::endl;
		return -1;
	}
	if (dict.item() != BILLION) {
		std::cout << "need billion dict" << std::endl;
		return 1;
	}

	const unsigned n = FLAGS_thread;
	constexpr unsigned batch = 5000;
	constexpr unsigned loop = 1000;

	uint64_t write_ops = 0;
	uint64_t write_ns = 0;
	bool quit = FLAGS_disable_write;
	std::thread writer([batch, &dict, &quit, &write_ops, &write_ns](){
		RandEmbGenerator writer(batch, BILLION);
		while (!quit) {
			auto start = std::chrono::steady_clock::now();
			dict.batch_update(writer);
			auto end = std::chrono::steady_clock::now();
			write_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
			write_ops += batch;
		}
	});

	std::vector<std::thread> workers;
	workers.reserve(n);
	std::vector<uint64_t> results(n);
	for (unsigned i = 0; i < n; i++) {
		workers.emplace_back([&dict, loop, batch](uint64_t* res){
			std::vector<uint64_t> key_vec(batch);
			auto out = std::make_unique<uint8_t[]>(EmbeddingGenerator::VALUE_SIZE*batch);

			XorShift128Plus rnd;
			uint64_t sum_ns = 0;
			for (unsigned i = 0; i < loop; i++) {
				for (unsigned j = 0; j < batch; j++) {
					key_vec[j] = rnd()%BILLION;
				}
				auto start = std::chrono::steady_clock::now();
				dict.batch_fetch(batch, (const uint8_t*)key_vec.data(), out.get());
				auto end = std::chrono::steady_clock::now();
				sum_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
			}
			*res = sum_ns;
		}, &results[i]);
	}
	for (auto& t : workers) {
		t.join();
	}

	quit = true;
	writer.join();

	uint64_t qps = 0;
	uint64_t ns = 0;
	for (auto x : results) {
		qps += (loop*batch)*1000000000ULL/x;
		ns += x;
	}
	ns /= n*(uint64_t)loop*(uint64_t)batch;

	std::cout << "read: " << (qps/1000000U) << " mqps with " << n << " threads" << std::endl;
	std::cout << "read: " << ns << " ns/op" << std::endl;
	if (!FLAGS_disable_write) {
		std::cout << "write: " << (write_ops*1000.0/write_ns) << " mqps" << std::endl;
	}
	return 0;
}

static int BenchBuild() {
	estuary::LuckyEstuary::Config config;
	config.entry = BILLION;
	config.capacity = BILLION;
	config.key_len = sizeof(uint64_t);
	config.val_len = EmbeddingGenerator::VALUE_SIZE;

	EmbeddingGenerator source(0, BILLION);
	if (!estuary::LuckyEstuary::Create(FLAGS_file, config, &source)) {
		std::cout << "fail to build" << std::endl;
		return 1;
	}
	return 0;
}

int main(int argc, char* argv[]) {
	google::ParseCommandLineFlags(&argc, &argv, true);

	auto cpus = get_nprocs();
	if (cpus <= 0) cpus = 1;
	if (FLAGS_thread == 0 || FLAGS_thread > cpus) {
		FLAGS_thread = cpus;
	}

	if (FLAGS_build) {
		return BenchBuild();
	} else {
		return BenchFetch();
	}
}