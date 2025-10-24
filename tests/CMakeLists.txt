set (OCTOPUS_TEST_BACKEND_ARGS "{\"bootstrap.servers\":\"localhost:9092\",\"session.timeout.ms\":10000}")
set (OCTOPUS_TEST_TOPIC_ARGS "{\"replication_factor\":1}")

add_test (NAME DiasporaTestSuite COMMAND
          diaspora-run-tests.sh octopus
                                --before ${CMAKE_CURRENT_SOURCE_DIR}/pre-test.sh
                                --after ${CMAKE_CURRENT_SOURCE_DIR}/post-test.sh
                                --backend-args ${OCTOPUS_TEST_BACKEND_ARGS}
                                --topic-args ${OCTOPUS_TEST_TOPIC_ARGS})
set_property (TEST DiasporaTestSuite PROPERTY
              ENVIRONMENT "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/src:$ENV{LD_LIBRARY_PATH}")
if (OFF)
add_test (NAME OctopusBenchmark COMMAND
          ${CMAKE_CURRENT_SOURCE_DIR}/run-benchmark.sh)
set_property (TEST OctopusBenchmark PROPERTY
              ENVIRONMENT
              "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/src:$ENV{LD_LIBRARY_PATH}"
              "PYTHONPATH=${CMAKE_SOURCE_DIR}/python/:${CMAKE_BINARY_DIR}/python:$ENV{PYTHONPATH}")
endif ()
