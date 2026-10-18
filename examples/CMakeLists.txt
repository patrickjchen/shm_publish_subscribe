foreach(ex basic_roundtrip shm_create shm_destroy shm_producer shm_consumer shm_admin shm_task)
  add_executable(${ex} ${ex}.cpp)
  target_link_libraries(${ex} PRIVATE shmctx::shmctx shmctx_warnings)
endforeach()
