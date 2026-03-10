#===============================================================================
# Dictionary designed for read-mostly scene.
# Copyright (C) 2020  Ruan Kunliang
# 
# This library is free software; you can redistribute it and/or modify it under 
# the terms of the GNU Lesser General Public License as published by the Free 
# Software Foundation; either version 2.1 of the License, or (at your option) 
# any later version.
#
# This library is distributed in the hope that it will be useful, but WITHOUT 
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License 
# along with the This Library; if not, see <https:#www.gnu.org/licenses/>.
#===============================================================================

cmake_minimum_required(VERSION 3.10)
project(estuary)

set(CMAKE_CXX_STANDARD 17)
include(CTest)
enable_testing()
find_package(Threads REQUIRED)
find_package(GTest REQUIRED)
find_package(gflags REQUIRED)

set(CMAKE_SKIP_BUILD_RPATH TRUE)
set(CMAKE_EXE_LINKER_FLAGS -Wl,--rpath=.)

set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG -Wall")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-unroll-loops -fno-stack-protector")

include_directories(${CMAKE_SOURCE_DIR}/include)

set(GFLAGS_TARGET gflags::gflags)
if(NOT TARGET ${GFLAGS_TARGET})
    if(TARGET gflags_shared)
        set(GFLAGS_TARGET gflags_shared)
    elseif(TARGET gflags_static)
        set(GFLAGS_TARGET gflags_static)
    elseif(TARGET gflags_nothreads_shared)
        set(GFLAGS_TARGET gflags_nothreads_shared)
    elseif(TARGET gflags_nothreads_static)
        set(GFLAGS_TARGET gflags_nothreads_static)
    else()
        message(FATAL_ERROR "Found gflags package but no supported CMake target was exported")
    endif()
endif()

file(GLOB source
	src/*.cc
)

add_library(estuary SHARED ${source})
target_link_libraries(estuary PRIVATE Threads::Threads)


file(GLOB test_src
	test/*.cc
)
add_executable(estuary-test ${test_src})
target_link_libraries(estuary-test PRIVATE Threads::Threads GTest::gtest estuary)
add_test(NAME estuary-test COMMAND estuary-test)

add_executable(lucky-billion benchmark/lucky-billion.cc)
target_link_libraries(lucky-billion PRIVATE Threads::Threads ${GFLAGS_TARGET} estuary)

add_executable(bench-estuary benchmark/bench-estuary.cc)
target_link_libraries(bench-estuary PRIVATE Threads::Threads ${GFLAGS_TARGET} estuary)
