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
var db = muxer.GetDatabase();

// Bloom Filter commands
var bloomFilter = new BloomFilterClient(db);
await bloomFilter.ReserveAsync(key, capacity, errorRate);
await bloomFilter.AddAsync(key, item);
await bloomFilter.AddMultipleAsync(key, item, item2);
await bloomFilter.InsertAsync(key, insertOptions, item, item2);
await bloomFilter.ExistsAsync(key, item);
await bloomFilter.ExistsMultipleAsync(key, item);
await bloomFilter.ScanDumpAsync(key, iterator);
await bloomFilter.LoadChunkAsync(key, iterator, dumpData);
await bloomFilter.InfoAsync(key);

// Cuckoo Filter commands
var cuckooFilter = new CuckooFilterClient(db);
await cuckooFilter.ReserveAsync(key, capacity);
await cuckooFilter.AddAsync(key, item);
await cuckooFilter.AddAdvancedAsync(key, item);
await cuckooFilter.InsertAsync(key, insertOptions, item, item2);
await cuckooFilter.InsertAdvanced(key, insertOptions, item, item2);
await cuckooFilter.ExistsAsync(key, item);
await cuckooFilter.DeleteAsync(key, item);
await cuckooFilter.CountAsync(key, item);
await cuckooFilter.ScanDumpAsync(key, iterator);
await cuckooFilter.LoadChunkAsync(key, iterator, dumpData);
await cuckooFilter.InfoAsync(key);

// Count-Min Sketch commands
var countMinSketch = new CountMinSketchClient(db);
await countMinSketch.InitByDimAsync(key, width, depth);
await countMinSketch.InitByProbAsync(key, error, probability);
await countMinSketch.IncrByAsync(key, item, increment);
await countMinSketch.QueryAsync(key, item);
await countMinSketch.MergeAsync(destinationKey, sourceKey);
await countMinSketch.InfoAsync(key);

// Top-K commands
var topk = new TopKClient(db);
await topk.ReserveAsync(key, topk, width, depth, decay);
await topk.AddAsync(key, item);
await topk.IncrementByAsync(key, item, increment);
await topk.QueryAsync(key, item);
await topk.CountAsync(key, item);
await topk.ListAsync(key);
await topk.InfoAsync(key);
```

There are a suite of tests in the repository that should be sufficient to serve as examples on how to use all supported commands.

Questions and Contributions
---

If you think you have found a bug or have a feature request, please [report an issue](https://github.com/yadazula/NRedisBloom/issues), or if appropriate, submit a pull request. If you have a question, feel free to [contact me](https://github.com/yadazula).

## License
[MIT](https://github.com/yadazula/NRedisBloom/blob/main/LICENSE)