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
project(estuary LANGUAGES CXX)

include(CTest)
option(ESTUARY_BUILD_BENCHMARKS "Build Estuary benchmarks" ON)
option(ESTUARY_ENABLE_TUNING "Use legacy compiler tuning flags, including -O2 for Release" ON)

find_package(Threads REQUIRED)

add_library(estuary SHARED
    src/estuary.cc
    src/hash.cc
    src/lucky_estuary.cc
    src/utils.cc
)
target_compile_features(estuary PUBLIC cxx_std_17)
target_include_directories(estuary PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>")
target_link_libraries(estuary PRIVATE Threads::Threads)

set(estuary_targets estuary)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    # FindGTest before CMake 3.20 exports GTest::GTest instead of GTest::gtest.
    if(TARGET GTest::gtest)
        set(estuary_gtest_target GTest::gtest)
    else()
        set(estuary_gtest_target GTest::GTest)
    endif()

    add_executable(estuary-test
        test/test.cc
        test/test_esuray.cc
        test/test_lucky.cc
        test/test_utils.cc
    )
    target_link_libraries(estuary-test PRIVATE Threads::Threads ${estuary_gtest_target} estuary)
    add_test(NAME estuary-test COMMAND estuary-test)
    list(APPEND estuary_targets estuary-test)
endif()

if(ESTUARY_BUILD_BENCHMARKS)
    find_package(gflags REQUIRED)
    set(estuary_gflags_target gflags::gflags)
    if(NOT TARGET ${estuary_gflags_target})
        if(TARGET gflags_shared)
            set(estuary_gflags_target gflags_shared)
        elseif(TARGET gflags_static)
            set(estuary_gflags_target gflags_static)
        elseif(TARGET gflags_nothreads_shared)
            set(estuary_gflags_target gflags_nothreads_shared)
        elseif(TARGET gflags_nothreads_static)
            set(estuary_gflags_target gflags_nothreads_static)
        else()
            message(FATAL_ERROR "Found gflags package but no supported CMake target was exported")
        endif()
    endif()

    add_executable(lucky-billion benchmark/lucky-billion.cc)
    target_link_libraries(lucky-billion PRIVATE Threads::Threads ${estuary_gflags_target} estuary)

    add_executable(bench-estuary benchmark/bench-estuary.cc)
    target_link_libraries(bench-estuary PRIVATE Threads::Threads ${estuary_gflags_target} estuary)
    list(APPEND estuary_targets lucky-billion bench-estuary)
endif()

foreach(estuary_target IN LISTS estuary_targets)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(${estuary_target} PRIVATE -Wall)
        if(ESTUARY_ENABLE_TUNING)
            target_compile_options(${estuary_target} PRIVATE
                "$<$<CONFIG:Release>:-O2>"
                -fno-unroll-loops
                -fno-stack-protector
            )
        endif()
    endif()
endforeach()
