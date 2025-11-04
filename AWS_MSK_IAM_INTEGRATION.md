# AWS MSK IAM Authentication Integration

This document describes the AWS MSK IAM authentication support added to Octopus.

## Overview

AWS MSK IAM authentication has been integrated throughout the Octopus library. When configured, all `rd_kafka_t` instances (producers, consumers, and admin clients) will automatically use AWS MSK IAM authentication via OAuth bearer tokens.

## Configuration

To enable AWS MSK IAM authentication, add an `aws_msk_iam` object to your Kafka configuration with the AWS region:

```json
{
  "kafka": {
    "bootstrap.servers": "your-msk-cluster.kafka.us-east-1.amazonaws.com:9098",
    "aws_msk_iam": {
      "region": "us-east-1"
    }
  }
}
```

## Implementation Details

### Authentication Setup

The authentication is configured automatically when:
1. The library is compiled with `ENABLE_MSK=ON` (defines `OCTOPUS_HAS_AWS`)
2. The configuration contains an `aws_msk_iam` object with a `region` field

### Modified Files

1. **src/KafkaHelper.hpp**
   - Added `setupAwsMskIamAuth()` function to configure Kafka with SASL_SSL and OAUTHBEARER
   - Added `applyAwsAuthIfConfigured()` function to check configuration and apply authentication
   - Updated `readFullTopic()` signature to accept JSON config and apply authentication
   - Updated `getNumEventsInPartition()` signature to accept JSON config and apply authentication

2. **src/Driver.cpp**
   - Updated `createTopic()` to apply authentication (line 45)
   - Updated `openTopic()` to apply authentication (line 163)
   - Updated `topicExists()` to apply authentication (line 231)

3. **src/TopicHandle.cpp**
   - Added include for `KafkaHelper.hpp`
   - Updated `makeProducer()` to apply authentication (line 39)
   - Updated `makeConsumer()` to apply authentication (line 106)

### Authentication Flow

When authentication is configured:

1. The Kafka configuration is set with:
   - `security.protocol` = `SASL_SSL`
   - `sasl.mechanisms` = `OAUTHBEARER`

2. An OAuth bearer token refresh callback is registered that:
   - Creates an `AwsMskIamSigner` with the specified region
   - Generates a signed AWS IAM token
   - Sets the token for Kafka authentication
   - Handles token refresh automatically (default 900 seconds expiration)

3. The callback uses AWS credentials from the environment:
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
   - `AWS_SESSION_TOKEN` (optional, for temporary credentials)

## Usage Example

### Basic Configuration

```cpp
#include <octopus/Driver.hpp>

// Configuration for AWS MSK
nlohmann::json config = {
    {"kafka", {
        {"bootstrap.servers", "your-cluster.kafka.us-east-1.amazonaws.com:9098"},
        {"aws_msk_iam", {
            {"region", "us-east-1"}
        }}
    }}
};

auto driver = octopus::OctopusDriver::create(diaspora::Metadata{config});
```

### Environment Variables

Ensure AWS credentials are available in the environment:

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"
```

## Compatibility

- **Compile-time**: Requires `ENABLE_MSK=ON` in CMake
- **Runtime**: Requires AWS SDK for C++ and AWS credentials in environment
- **Without AWS**: If compiled without `ENABLE_MSK`, the authentication configuration is safely ignored

## Testing

The implementation follows the pattern from `msk_iam_example/producer.cpp` and `msk_iam_example/consumer.cpp`, which have been tested with AWS MSK clusters.

## Notes

- Token expiration defaults to 900 seconds (15 minutes)
- Tokens are automatically refreshed by librdkafka
- The AWS region must match your MSK cluster's region
- The implementation uses `AwsMskIamSigner` class for token generation
- All `rd_kafka_t` instances created by Octopus will use authentication when configured
