# Catch2 v3: prefer a system/Conan package, otherwise fetch a pinned release.
find_package(Catch2 3 QUIET)
if(NOT Catch2_FOUND)
  include(FetchContent)
  FetchContent_Declare(Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG        v3.7.1
    GIT_SHALLOW    TRUE)
  FetchContent_MakeAvailable(Catch2)
  list(APPEND CMAKE_MODULE_PATH ${catch2_SOURCE_DIR}/extras)
endif()
include(Catch)

# Shared test support (unique SysV key files, cleanup, payload helpers).
add_library(shmctx_test_support INTERFACE)
target_include_directories(shmctx_test_support INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/support)
target_link_libraries(shmctx_test_support INTERFACE shmctx::shmctx shmctx_warnings)

add_subdirectory(unit)
add_subdirectory(integration)
add_subdirectory(large)
