# Octopus

This repository contains a Kafka-based implementation of the
[Diaspora Streaming API](https://github.com/diaspora-project/diaspora-stream-api).

## Installing

To build this backend, you need to use [Spack](https://spack.io) and the
[Diaspora](https://github.com/diaspora-project/diaspora-spack-packages) repository of Spack packages.
After the Diaspora package repository has been set up, you can build Octopus using:

```
spack install octopus
```

By default, Octopus will be installed with support for AWS MSK IAM authentication.
To disable it, install `octopus~msk`.

## Using the Octopus backend.

Octopus is a backend for the [Diaspora Streaming API](https://github.com/diaspora-project/diaspora-stream-api).
As such, it can be used by instantiating `diaspora::Driver` (in C++) as follows.

```cpp
#include <diaspora/Driver.hpp>
#include <nlohmann/json.hpp>

...

auto nlohmann::json config = ...;
auto driver = diaspora::Driver::New("octopus", config);
```

The configuration should be a JSON object with at minimum the following information.

```json
{
    "kafka": {
        "bootstrap.servers": [ "host1:port1", "host2:port2", ...],
        ...
    },
    ...
}
```

Properties inside the "kafka" entry will be passed down to
[librdkafka](https://github.com/confluentinc/librdkafka) when instantiating `rd_kafka_t` objects.

To instantiate an Octopus driver in Python, use the following.

```python
from diaspora_stream.api import Driver

config = { ... }
driver = Driver(backend="octopus", options=config)
```

IMPORTANT: In both Python and C++, `liboctopus.so` must be available in `LD_LIBRARY_PATH` for the
Diaspora Stream API to be able to load it.

## Octopus cloud service

By default, the driver performs topic admin operations (create, list, exists, delete)
directly against Kafka using librdkafka. To use the Octopus cloud service instead,
add an `"octopus"` object to the configuration:

```json
{
    "kafka": {
        "bootstrap.servers": "host:port"
    },
    "namespace": "my-namespace",
    "octopus": {
        "subject": "my-subject-id",
        "authorization": "my-auth-token",
        "url": "https://my-octopus-service.example.com"
    }
}
```

When the `"octopus"` field is present, topic admin operations are routed through the
Octopus REST API, while data-plane operations (produce, consume) still go directly
through Kafka.

### Fields

| Field | Required | Description |
|---|---|---|
| `subject` | Yes (or `subject_env`) | The subject identifier sent in the `subject` HTTP header. |
| `subject_env` | Yes (or `subject`) | Name of an environment variable from which to read the subject value. |
| `authorization` | Yes (or `authorization_env`) | The authorization token sent in the `authorization` HTTP header. |
| `authorization_env` | Yes (or `authorization`) | Name of an environment variable from which to read the authorization value. |
| `url` | No | Base URL of the Octopus service. Defaults to the production endpoint if omitted. |

For `subject` and `authorization`, you can provide the value directly or reference an
environment variable using the `_env` variant. If both are present (e.g. both `subject`
and `subject_env`), the `_env` variant takes priority.

### Example using environment variables

```json
{
    "kafka": {
        "bootstrap.servers": "host:port"
    },
    "namespace": "my-namespace",
    "octopus": {
        "subject_env": "OCTOPUS_SUBJECT",
        "authorization_env": "OCTOPUS_AUTH"
    }
}
```

```bash
export OCTOPUS_SUBJECT="my-subject-id"
export OCTOPUS_AUTH="my-auth-token"
```
