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

#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <utils.h>
#include "internal.h"

namespace estuary {

struct DefaultLogger : public Logger {
	void printf(const char* format, va_list args) override;
	static DefaultLogger instance;
};
void DefaultLogger::printf(const char *format, va_list args) {
	::vfprintf(stderr, format, args);
}
DefaultLogger DefaultLogger::instance;
Logger* Logger::s_instance = &DefaultLogger::instance;

void Logger::Printf(const char* format, ... ) {
	if (s_instance != nullptr) {
		va_list args;
		va_start(args, format);
		s_instance->printf(format, args);
		va_end(args);
	}
}

MemMap::MemMap(const char* path, bool populate, bool exclusive, size_t size) noexcept {
	int fd = -1;
	if (size == 0) {
		fd = open(path, O_RDWR);
	} else {
		fd = open(path, O_RDWR | O_CREAT, 0644);
	}
	if (fd < 0) {
		Logger::Printf("fail to open file: %s\n", path);
		return;
	}
	if (flock(fd, LOCK_NB|(exclusive?LOCK_EX:LOCK_SH)) != 0) {
		Logger::Printf("fail to lock file: %s\n", path);
		close(fd);
		return;
	}
	if (size == 0) {
		struct stat stat;
		if (fstat(fd, &stat) != 0 || stat.st_size <= 0) {
			Logger::Printf("fail to read file: %s\n", path);
			close(fd);
			return;
		}
		size = stat.st_size;
	} else {
		if (ftruncate64(fd, 0) != 0 || ftruncate64(fd, size) != 0) {
			Logger::Printf("fail to write file: %s\n", path);
			close(fd);
			return;
		}
	}
	auto addr = mmap(nullptr, size, PROT_READ|PROT_WRITE,
					 populate? MAP_SHARED|MAP_POPULATE : MAP_SHARED, fd, 0);
	if (addr == MAP_FAILED) {
		close(fd);
		return;
	}
	m_addr = static_cast<uint8_t*>(addr);
	m_size = size;
	m_fd = fd;
}

static inline constexpr size_t RoundUp(size_t n) {
	constexpr size_t m = 0x1fffff;
	return (n+m)&(~m);
};

static bool Read(int fd, uint8_t* data, size_t size) noexcept {
	constexpr size_t block = 16*1024*1024;
	size_t off = 0;
	while (size > block) {
		auto next = off + block;
		readahead(fd, next, block);
		if (pread(fd, data, block, off) != block) {
			return false;
		}
		off = next;
		data += block;
		size -= block;
	}
	if (pread(fd, data, size, off) != size) {
		return false;
	}
	return true;
}

MemMap::MemMap(const char* path, LoadByCopy) {
	auto fd = open(path, O_RDWR);
	if (fd < 0) {
		Logger::Printf("fail to open file: %s\n", path);
		return;
	}
	if (flock(fd, LOCK_NB|LOCK_EX) != 0) {
		Logger::Printf("fail to lock file: %s\n", path);
		close(fd);
		return;
	}
	struct stat stat;
	if (fstat(fd, &stat) != 0 || stat.st_size <= 0) {
		Logger::Printf("fail to read file: %s\n", path);
		close(fd);
		return;
	}
  auto size = stat.st_size;
  new(this)MemMap(size, [fd, size, path](uint8_t* space)->bool {
    if (!Read(fd, space, size)) {
      Logger::Printf("fail to read file: %s\n", path);
      return false;
    }
    return true;
  });
	close(fd);
}

MemMap::MemMap(size_t size, const std::function<bool(uint8_t*)>& load) {
  if (size == 0) {
    Logger::Printf("unexpected size 0\n");
    return;
  }
  auto round_up_size = RoundUp(size);
  void* addr = mmap(nullptr, round_up_size, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
  if (addr == MAP_FAILED && errno == ENOMEM) {
    addr = mmap(nullptr, round_up_size, PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  }
  if (addr == MAP_FAILED) {
    Logger::Printf("fail to mmap[%d]: %lu\n", errno, round_up_size);
    return;
  }
  if (!load((uint8_t*)addr)) {
    munmap(addr, round_up_size);
    return;
  }
  m_addr = static_cast<uint8_t*>(addr);
  m_size = size;
}

MemMap::~MemMap() noexcept {
	if (m_addr != nullptr) {
		auto size = m_fd >= 0? m_size : RoundUp(m_size);
		if (munmap(m_addr, size) != 0) {
			Logger::Printf("fail to munmap[%d]: %p | %lu\n", errno, m_addr, m_size);
		};
	}
	if (m_fd >= 0) {
		close(m_fd);
	}
}

bool MemMap::dump(const char* path) const noexcept {
	if (!*this) {
		return false;
	}
	auto fd = open(path, O_CREAT|O_TRUNC|O_WRONLY, 0644);
	if (fd < 0) {
		Logger::Printf("fail to open file: %s\n", path);
		return false;
	}
	ssize_t remain = m_size;
	for (auto buf = m_addr; remain > 0;) {
		auto sz = write(fd, buf, remain);
		if (sz < 0) {
			break;
		}
		buf += sz;
		remain -= sz;
	}
	close(fd);
	return remain == 0;
}

} //estuary
