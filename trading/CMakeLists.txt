# Algo trading skeleton built on shmctx: message routing only.
add_library(trading INTERFACE)
add_library(trading::trading ALIAS trading)
target_include_directories(trading INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(trading INTERFACE shmctx::shmctx)

foreach(app gateway oms algo_engine ems exchange_sim md_handler persister client_sim)
  add_executable(trading_${app} src/${app}_main.cpp)
  target_link_libraries(trading_${app} PRIVATE trading::trading shmctx_warnings)
endforeach()

if(SHMCTX_BUILD_TESTS AND TARGET Catch2::Catch2WithMain)
  add_executable(trading_routing_tests tests/routing_tests.cpp)
  target_link_libraries(trading_routing_tests PRIVATE trading::trading shmctx_test_support Catch2::Catch2WithMain)
  catch_discover_tests(trading_routing_tests TEST_PREFIX "trading." PROPERTIES LABELS trading)

  add_test(NAME trading.system
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/scripts/run_system.sh $<TARGET_FILE_DIR:trading_gateway> ${CMAKE_CURRENT_SOURCE_DIR}/config/system.ini 2000)
  set_tests_properties(trading.system PROPERTIES LABELS trading TIMEOUT 120)
endif()
