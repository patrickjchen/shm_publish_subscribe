cmake_minimum_required(VERSION 3.20)
project(shmctx VERSION 0.1.0 LANGUAGES CXX)

# ---------------------------------------------------------------------------
# shmctx: header-only lock-free multi-producer / multi-consumer SHM queue,
# vendored from the ATLA monorepo's core-libraries/include/shm_context.h.
# ---------------------------------------------------------------------------

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

option(SHMCTX_BUILD_EXAMPLES "Build example binaries" ON)
option(SHMCTX_BUILD_TESTS    "Build unit + integration tests" ON)
option(SHMCTX_SANITIZE       "Build tests with ASan/UBSan" OFF)
option(SHMCTX_BUILD_TRADING  "Build the algo-trading routing skeleton (trading/)" ON)

add_library(shmctx INTERFACE)
add_library(shmctx::shmctx ALIAS shmctx)
target_include_directories(shmctx SYSTEM INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)
target_compile_features(shmctx INTERFACE cxx_std_20)
find_package(Threads REQUIRED)
target_link_libraries(shmctx INTERFACE Threads::Threads)

# Warnings for everything we compile ourselves (examples / tests).
add_library(shmctx_warnings INTERFACE)
target_compile_options(shmctx_warnings INTERFACE
  -Wall -Wextra -Wpedantic -Wno-unused-parameter)
if(SHMCTX_SANITIZE)
  target_compile_options(shmctx_warnings INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(shmctx_warnings INTERFACE -fsanitize=address,undefined)
endif()

if(SHMCTX_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(SHMCTX_BUILD_TESTS)
  include(CTest)
  enable_testing()
  add_subdirectory(tests)
endif()

if(SHMCTX_BUILD_TRADING)
  add_subdirectory(trading)
endif()

include(GNUInstallDirs)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(TARGETS shmctx EXPORT shmctxTargets)
install(EXPORT shmctxTargets NAMESPACE shmctx:: DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/shmctx)
