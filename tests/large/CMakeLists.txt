# Each test here commits ~1.1 GiB; RESOURCE_LOCK keeps them (and the other
# big-memory suites) from running at the same time under ctest -j.
add_executable(shmctx_large_tests large_queue_tests.cpp)
target_link_libraries(shmctx_large_tests PRIVATE shmctx_test_support Catch2::Catch2WithMain)
catch_discover_tests(shmctx_large_tests TEST_PREFIX "large." PROPERTIES LABELS large RESOURCE_LOCK bigmem TIMEOUT 600)
