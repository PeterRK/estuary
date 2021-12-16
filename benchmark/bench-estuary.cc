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
#include <string>
#include <chrono>
#include <sys/sysinfo.h>
#include <estuary.h>
#include <gflags/gflags.h>
#include "benchmark.h"

DEFINE_string(file, "bench.es", "dict filename");
DEFINE_uint32(thread, 4, "number of worker threads");
DEFINE_bool(build, false, "build instead of fetching");
DEFINE_bool(copy, false, "load by copy");
DEFINE_bool(disable_write, false, "disable write");
DEFINE_bool(disable_pipeline, false, "disable pipeline");

static constexpr size_t SIZE = 1UL << 27U;

static int BenchFetch() {
	auto mode = FLAGS_copy? estuary::Estuary::COPY_DATA : estuary::Estuary::MONOPOLY;
	auto dict = estuary::Estuary::Load(FLAGS_file, mode);
	if (!dict) {
		std::cout << "fail to load: " << FLAGS_file << std::endl;
		return -1;
	}
	if (dict.item() != SIZE) {
		std::cout << "unexpected size" << std::endl;
		return 1;
	}

	uint64_t write_ops = 0;
	uint64_t write_ns = 0;
	volatile bool quit = FLAGS_disable_write;
	std::thread writer([ &dict, &quit, &write_ops, &write_ns](){
		XorShift128Plus rnd;
		uint64_t key = 0;
		uint8_t val[UINT8_MAX];
		uint8_t len = 0;
		auto arr = (uint64_t*)val;
		for (unsigned i = 0; i < (UINT8_MAX+1)/8; i++) {
			arr[i] = rnd();
		}

		auto start = std::chrono::steady_clock::now();
		for (; !quit; write_ops++) {
			key = rnd() % SIZE;
			dict.update({(const uint8_t*)&key, sizeof(uint64_t)}, {val, len++});
		}
		auto end = std::chrono::steady_clock::now();
		write_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
	});

	const unsigned n = FLAGS_thread;
	constexpr unsigned loop = 1000000;

	std::vector<std::thread> workers;
	workers.reserve(n);
	std::vector<uint64_t> results(n);
	for (unsigned i = 0; i < n; i++) {
		workers.emplace_back([&dict](uint64_t* res){
			XorShift128Plus rnd;
			std::string val;
			auto start = std::chrono::steady_clock::now();
			if (FLAGS_disable_pipeline || loop < 2) {
				for (unsigned i = 0; i < loop; i++) {
					uint64_t key = rnd() % SIZE;
					dict.fetch({(const uint8_t*)&key, sizeof(uint64_t)}, val);
				}
			} else {
				struct Context {
					uint64_t key;
					uint64_t code;
					Context(XorShift128Plus& rnd, const estuary::Estuary& dict) noexcept {
						key = rnd() % SIZE;
						code = dict.touch({(const uint8_t*)&key, sizeof(uint64_t)});
					}
				};
				Context a(rnd, dict);
				Context b(rnd, dict);
				dict.touch(a.code);
				for (unsigned i = 2; i < loop; i++) {
					Context c(rnd, dict);
					dict.touch(b.code);
					dict.fetch(a.code, {(const uint8_t*)&a.key, sizeof(uint64_t)}, val);
					a = b;
					b = c;
				}
				dict.touch(b.code);
				dict.fetch(a.code, {(const uint8_t*)&a.key, sizeof(uint64_t)}, val);
				dict.fetch(b.code, {(const uint8_t*)&b.key, sizeof(uint64_t)}, val);
			}
			auto end = std::chrono::steady_clock::now();
			*res = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
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
		qps += loop*1000000000ULL/x;
		ns += x;
	}
	ns /= n*(uint64_t)loop;

	std::cout << "read: " << (qps/1000000.0) << " mqps with " << n << " threads" << std::endl;
	std::cout << "read: " << ns << " ns/op" << std::endl;
	if (!FLAGS_disable_write) {
		std::cout << "write: " << (write_ops * 1000.0 / write_ns) << " mqps" << std::endl;
	}
	return 0;
}

static int BenchBuild() {
	estuary::Estuary::Config config;
	config.item_limit = SIZE;
	config.max_key_len = sizeof(uint64_t);
	config.max_val_len = UINT8_MAX;
	config.avg_item_size = UINT8_MAX / 2 + 1 + sizeof(uint64_t);

	VariedValueGenerator source(0, SIZE);
	if (!estuary::Estuary::Create(FLAGS_file, config, &source)) {
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

