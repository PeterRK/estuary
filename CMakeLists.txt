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

set(CMAKE_SKIP_BUILD_RPATH TRUE)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-unroll-loops -mbmi2")
set(CMAKE_EXE_LINKER_FLAGS -Wl,--rpath=.)

include_directories(${CMAKE_SOURCE_DIR}/include)

file(GLOB source
	src/*.cc
)

add_library(estuary SHARED ${source})
target_link_libraries(estuary pthread)


file(GLOB test_src
	test/*.cc
)
add_executable(test ${test_src})
target_link_libraries(test pthread gtest estuary)

add_executable(lucky-billion benchmark/lucky-billion.cc)
target_link_libraries(lucky-billion pthread gflags estuary)

add_executable(bench-estuary benchmark/bench-estuary.cc)
target_link_libraries(bench-estuary pthread gflags estuary)