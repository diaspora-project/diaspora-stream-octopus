# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Octopus is a Kafka-based implementation of the [Diaspora Streaming API](https://github.com/diaspora-project/diaspora-stream-api). It provides a C++ library that wraps librdkafka to implement the Diaspora streaming interface, enabling distributed event streaming with topic management, producers, and consumers.

## Build System

This project uses CMake (minimum version 3.21) with Spack for dependency management.

### Dependencies

Core dependencies managed via Spack (`spack.yaml`):
- `diaspora-stream-api` - Core streaming API interface
- `librdkafka` - Apache Kafka C/C++ client library
- `pkg-config` - For finding libraries
- `uuid` - UUID generation
- `aws-sdk-cpp` - Optional, for AWS MSK IAM authentication (ENABLE_MSK)

### Build Commands

```bash
# Configure with CMake (from build directory)
cmake .. -DCMAKE_BUILD_TYPE=Release

# With tests enabled
cmake .. -DENABLE_TESTS=ON

# With AWS support
cmake .. -DENABLE_MSK=ON

# With coverage
cmake .. -DENABLE_COVERAGE=ON

# Build
cmake --build .

# Install
cmake --install . --prefix /path/to/install
```

### Testing

```bash
# Run all tests (requires ENABLE_TESTS=ON)
ctest

# Run specific test suites
ctest -R DiasporaTestSuite
ctest -R OctopusBenchmark

# Tests require a running Kafka instance (handled by pre-test.sh/post-test.sh)
```

The test infrastructure automatically:
1. Starts a local Kafka server (via `tests/pre-test.sh`)
2. Runs the Diaspora test suite using `diaspora-run-tests.sh`
3. Runs producer/consumer benchmarks
4. Cleans up Kafka server (via `tests/post-test.sh`)

## Architecture

### Core Components

The library implements the Diaspora Streaming API using Apache Kafka as the backend:

1. **OctopusDriver** (`src/Driver.cpp`, `include/octopus/Driver.hpp`)
   - Main entry point implementing `diaspora::DriverInterface`
   - Handles topic creation with metadata storage in `__info_<topic_name>` topics
   - Manages Kafka configuration via nested JSON: `{"kafka": {"bootstrap.servers": "..."}}`
   - Registered with Diaspora via `DIASPORA_REGISTER_DRIVER(octopus, octopus, OctopusDriver)`

2. **OctopusTopicHandle** (`src/TopicHandle.cpp`, `include/octopus/TopicHandle.hpp`)
   - Represents an opened topic
   - Stores validator, partition selector, and serializer metadata
   - Creates producers and consumers for the topic
   - Queries Kafka metadata to determine partition count

3. **OctopusProducer** (`src/Producer.cpp`, `include/octopus/Producer.hpp`)
   - Implements `diaspora::ProducerInterface`
   - Wraps `rd_kafka_t` producer handle
   - Async message delivery with callback handling
   - Tracks pending messages atomically for flush operations

4. **OctopusConsumer** (`src/Consumer.cpp`, `include/octopus/Consumer.hpp`)
   - Implements `diaspora::ConsumerInterface`
   - Wraps `rd_kafka_t` consumer handle
   - Supports both pull-based and process-based consumption patterns
   - Handles partition assignment for target partitions

5. **KafkaConf** (`include/octopus/KafkaConf.hpp`)
   - RAII wrapper around `rd_kafka_conf_t`
   - Converts JSON configuration to librdkafka properties
   - Handles arrays and objects in configuration (e.g., `bootstrap.servers` can be string or array)

6. **OctopusThreadPool** (`include/octopus/ThreadPool.hpp`)
   - Implements `diaspora::ThreadPoolInterface`
   - ThreadCount{0} creates a default thread pool

### Kafka Integration Details

- **Topic Creation**: Creates both the main topic and an info topic (`__info_<name>`) to store validator, selector, and serializer metadata
- **Configuration Format**: All Kafka settings are nested under `"kafka"` key in JSON config
- **Metadata Storage**: Topic metadata (validator, selector, serializer) is written to info topic and read on `openTopic()`
- **Partition Discovery**: Uses `rd_kafka_metadata()` to query actual partition count from Kafka

### AWS MSK IAM Authentication

When built with `ENABLE_MSK=ON`, the library supports AWS MSK IAM authentication:

- **Configuration**: Add `"aws_msk_iam": {"region": "us-east-1"}` to the Kafka configuration
- **Implementation**: Uses `AwsMskIamSigner` class to generate OAuth bearer tokens
- **Automatic**: All `rd_kafka_t` instances automatically use authentication when configured
- **Token Refresh**: Tokens are automatically refreshed by librdkafka (default 900s expiration)
- **Credentials**: Uses AWS credentials from environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)

Example configuration:
```json
{
  "kafka": {
    "bootstrap.servers": "your-cluster.kafka.us-east-1.amazonaws.com:9098",
    "aws_msk_iam": {
      "region": "us-east-1"
    }
  }
}
```

See `AWS_MSK_IAM_INTEGRATION.md` for detailed integration documentation.

## File Structure

```
include/octopus/          # Public headers
  ├── Driver.hpp          # Main driver interface
  ├── TopicHandle.hpp     # Topic handle
  ├── Producer.hpp        # Producer interface
  ├── Consumer.hpp        # Consumer interface
  ├── KafkaConf.hpp       # Kafka configuration wrapper
  ├── ThreadPool.hpp      # Thread pool implementation
  └── Event.hpp           # Event types

src/                      # Implementation files
  ├── Driver.cpp          # Driver implementation with topic management
  ├── TopicHandle.cpp     # Topic handle implementation
  ├── Producer.cpp        # Producer implementation
  ├── Consumer.cpp        # Consumer implementation
  └── AwsMskIamSigner.cpp # AWS IAM authentication (optional)

tests/                    # Test infrastructure
  ├── pre-test.sh         # Starts local Kafka server
  ├── post-test.sh        # Stops Kafka and cleanup
  └── run-benchmark.sh    # Runs producer/consumer benchmarks
```

## Development Workflow

### Working with Kafka Configuration

The driver expects configuration in this format:
```json
{
  "kafka": {
    "bootstrap.servers": "localhost:9092",
    "other.kafka.property": "value"
  }
}
```

The `"bootstrap.servers"` property can be:
- A string: `"host1:9092"`
- An array: `["host1:9092", "host2:9092"]` (automatically joined with commas)

### Adding New Features

When modifying the library:
1. Changes to public API should update headers in `include/octopus/`
2. Implementation goes in `src/`
3. Ensure compatibility with Diaspora Streaming API interface
4. Consider both producer and consumer code paths
5. Test with actual Kafka using the test infrastructure

### Debugging Tests

If tests fail, check:
- `kafka.<CLUSTER_ID>.out` and `kafka.<CLUSTER_ID>.err` for Kafka server logs
- Kafka must be installed via Spack for `kafka-storage.sh`, `kafka-server-start.sh` commands
- The test environment sets `LD_LIBRARY_PATH` to find the built library
