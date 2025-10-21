set (DDD_TEST_BACKEND_ARGS "{}")
set (DDD_TEST_TOPIC_ARGS "{}")

add_test (NAME DiasporaTestSuite COMMAND
          diaspora-run-tests.sh BBB
                                --before ${CMAKE_CURRENT_SOURCE_DIR}/pre-test.sh
                                --after ${CMAKE_CURRENT_SOURCE_DIR}/post-test.sh
                                --backend-args ${DDD_TEST_BACKEND_ARGS}
                                --topic-args ${DDD_TEST_TOPIC_ARGS})
set_property (TEST DiasporaTestSuite PROPERTY
              ENVIRONMENT "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/src:$ENV{LD_LIBRARY_PATH}")

add_test (NAME CCCBenchmark COMMAND
          ${CMAKE_CURRENT_SOURCE_DIR}/run-benchmark.sh)
set_property (TEST CCCBenchmark PROPERTY
              ENVIRONMENT
              "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/src:$ENV{LD_LIBRARY_PATH}"
              "PYTHONPATH=${CMAKE_SOURCE_DIR}/python/:${CMAKE_BINARY_DIR}/python:$ENV{PYTHONPATH}")
