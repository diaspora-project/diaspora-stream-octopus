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
        "bootstrap.servers": [ "host1:port1", "host2:port2, ...],
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
