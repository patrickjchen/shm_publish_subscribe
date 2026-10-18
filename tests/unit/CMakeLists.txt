add_executable(shmctx_unit_tests
  context_tests.cpp
  produce_consume_tests.cpp
  topic_tests.cpp
  producer_wrappers_tests.cpp
  concurrency_tests.cpp
  topic_mask_tests.cpp
  ini_tests.cpp
  task_tests.cpp)
target_link_libraries(shmctx_unit_tests PRIVATE shmctx_test_support Catch2::Catch2WithMain)
catch_discover_tests(shmctx_unit_tests TEST_PREFIX "unit." PROPERTIES LABELS unit)
