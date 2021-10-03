[![Build](https://github.com/yadazula/NRedisBloom/actions/workflows/CI.yml/badge.svg)](https://github.com/yadazula/NRedisBloom/actions/workflows/CI.yml)
[![codecov](https://codecov.io/gh/yadazula/NRedisBloom/branch/main/graph/badge.svg)](https://codecov.io/gh/yadazula/NRedisBloom)

# NRedisBloom

[![Forum](https://img.shields.io/badge/Forum-RedisBloom-blue)](https://forum.redislabs.com/c/modules/redisbloom)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/wXhwjCQ)

A .NET client library for [RedisBloom](https://redisbloom.io/) probabilistic module, based on [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis).

## Overview

This project contains a .NET library abstracting the API of the [RedisBloom](https://redisbloom.io/) Redis module, that provides four data structures: a scalable bloom filter, a cuckoo filter, a count-min sketch, and a top-k.

See [redisbloom.io](https://redisbloom.io) for installation instructions of the module and documentation about the available commands.

## Requirements
- Redis server 4.0+ version (Redis Modules are only available from Redis 4.0+)
- RedisBloom Module installed on Redis server as specified in [Building and running](https://oss.redislabs.com/redisbloom/Quick_Start/#building-and-running)

## Installation

```PowerShell
PM> Install-Package NRedisBloom
```

## Usage Example

```csharp
// Create redis connection
var muxer = ConnectionMultiplexer.Connect(options);
var redis = muxer.GetDatabase();

// Bloom Filter commands
await redis.BloomFilterReserveAsync(key, capacity, errorRate);
await redis.BloomFilterAddAsync(key, item);
await redis.BloomFilterAddMultipleAsync(key, item, item2);
await redis.BloomFilterInsertAsync(key, insertOptions, item, item2);
await redis.BloomFilterExistsAsync(key, item);
await redis.BloomFilterExistsMultipleAsync(key, item);
await redis.BloomFilterScanDumpAsync(key, iterator);
await redis.BloomFilterLoadChunkAsync(key, iterator, dumpData);
await redis.BloomFilterInfoAsync(key);

// Cuckoo Filter commands
await redis.CuckooFilterReserveAsync(key, capacity);
await redis.CuckooFilterAddAsync(key, item);
await redis.CuckooFilterAddAdvancedAsync(key, item);
await redis.CuckooFilterInsertAsync(key, insertOptions, item, item2);
await redis.CuckooFilterInsertAdvanced(key, insertOptions, item, item2);
await redis.CuckooFilterExistsAsync(key, item);
await redis.CuckooFilterDeleteAsync(key, item);
await redis.CuckooFilterCountAsync(key, item);
await redis.CuckooFilterScanDumpAsync(key, iterator);
await redis.CuckooFilterLoadChunkAsync(key, iterator, dumpData);
await redis.CuckooFilterInfoAsync(key);

// Count-Min Sketch commands
await redis.CountMinSketchInitByDimAsync(key, width, depth);
await redis.CountMinSketchInitByProbAsync(key, error, probability);
await redis.CountMinSketchIncrByAsync(key, item, increment);
await redis.CountMinSketchQueryAsync(key, item);
await redis.CountMinSketchMergeAsync(destinationKey, sourceKey);
await redis.CountMinSketchInfoAsync(key);

// Top-K commands
await redis.TopKReserveAsync(key, topk, width, depth, decay);
await redis.TopKAddAsync(key, item);
await redis.TopKIncrementByAsync(key, item, increment);
await redis.TopKQueryAsync(key, item);
await redis.TopKCountAsync(key, item);
await redis.TopKListAsync(key);
await redis.TopKInfoAsync(key);
```

Questions and Contributions
---

If you think you have found a bug or have a feature request, please [report an issue](https://github.com/yadazula/NRedisBloom/issues), or if appropriate, submit a pull request. If you have a question, feel free to [contact me](https://github.com/yadazula).

## License
[MIT](https://github.com/yadazula/NRedisBloom/blob/main/LICENSE)