# Multi-process tests: real SysV shared memory, fork()ed producers/consumers,
# and a rogue child that dies between claim and commit to exercise recovery.
add_executable(shmctx_integration_tests
  multiprocess_tests.cpp
  crash_recovery_tests.cpp)
target_compile_definitions(shmctx_integration_tests PRIVATE SHMCTX_ENABLE_TEST_HOOKS)
target_link_libraries(shmctx_integration_tests PRIVATE shmctx_test_support Catch2::Catch2WithMain)
catch_discover_tests(shmctx_integration_tests TEST_PREFIX "integration." PROPERTIES LABELS integration)

# End-to-end CLI pipeline using the example binaries, and shm_task pipelines.
if(SHMCTX_BUILD_EXAMPLES)
  add_executable(shmctx_pipeline_tests pipeline_tests.cpp)
  target_compile_definitions(shmctx_pipeline_tests PRIVATE SHM_TASK_BIN="$<TARGET_FILE:shm_task>")
  target_link_libraries(shmctx_pipeline_tests PRIVATE shmctx_test_support Catch2::Catch2WithMain)
  add_dependencies(shmctx_pipeline_tests shm_task)
  # Pipelines create 1 GiB queues: never two at once. NOTE: PROPERTIES must be
  # a flat list; a multi-value LABELS ("a;b") expands to two words in the
  # generated file and silently breaks every property after it.
  catch_discover_tests(shmctx_pipeline_tests TEST_PREFIX "pipeline." PROPERTIES LABELS pipeline RESOURCE_LOCK bigmem TIMEOUT 600)

  add_test(NAME integration.cli_pipeline
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/cli_pipeline.sh $<TARGET_FILE_DIR:shm_producer>)
  set_tests_properties(integration.cli_pipeline PROPERTIES LABELS integration TIMEOUT 300 RESOURCE_LOCK bigmem SKIP_RETURN_CODE 77)
endif()
