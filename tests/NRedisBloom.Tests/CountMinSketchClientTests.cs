using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using NRedisBloom.CountMinSketch;
using StackExchange.Redis;
using Xunit;

namespace NRedisBloom.Tests
{
    public class CountMinSketchClientTests : TestBase
    {
        private readonly CountMinSketchClient _client;

        public CountMinSketchClientTests()
        {
            _client = new CountMinSketchClient(Db);
        }

        [Fact]
        public void InitByDimBasic()
        {
            const int width = 2000;
            const int depth = 5;

            var keyName = KeyName();

            Assert.True(_client.InitByDim(keyName, width, depth));

            var sketchInfo = _client.Info(keyName);
            Assert.Equal(width, sketchInfo.Width);
            Assert.Equal(depth, sketchInfo.Depth);
            Assert.Equal(default, sketchInfo.Count);
        }

        [Fact]
        public async Task InitByDimBasicAsync()
        {
            const int width = 2000;
            const int depth = 5;

            var keyName = KeyName();

            Assert.True(await _client.InitByDimAsync(keyName, width, depth));

            var sketchInfo = await _client.InfoAsync(keyName);
            Assert.Equal(width, sketchInfo.Width);
            Assert.Equal(depth, sketchInfo.Depth);
            Assert.Equal(default, sketchInfo.Count);
        }

        [Fact]
        public void InitByProbBasic()
        {
            var keyName = KeyName();

            Assert.True(_client.InitByProb(keyName, 0.001, 0.01));

            var sketchInfo = _client.Info(keyName);
            Assert.Equal(default, sketchInfo.Count);
        }

        [Fact]
        public async Task InitByProbBasicAsync()
        {
            var keyName = KeyName();

            Assert.True(await _client.InitByProbAsync(keyName, 0.001, 0.01));

            var sketchInfo = await _client.InfoAsync(keyName);
            Assert.Equal(default, sketchInfo.Count);
        }

        [Fact]
        public void IncrBySingleItem()
        {
            const int increment = 10;

            var keyName = KeyName();

            _client.InitByDim(keyName, 2000, 5);

            var count = _client.IncrBy(keyName, "foo", increment);

            Assert.True(count >= increment);
        }

        [Fact]
        public async Task IncrBySingleItemAsync()
        {
            const int increment = 10;

            var keyName = KeyName();

            await _client.InitByDimAsync(keyName, 2000, 5);

            var count = await _client.IncrByAsync(keyName, "foo", increment);

            Assert.True(count >= increment);
        }

        [Fact]
        public void IncrByMultipleItems()
        {
            const int increment1 = 10;
            const int increment2 = 42;

            var keyName = KeyName();

            _client.InitByDim(keyName, 2000, 5);

            var counts = _client.IncrBy(keyName,
                new Dictionary<string, long> {{"foo", increment1}, {"bar", increment2}});

            Assert.True(counts[0] >= increment1);
            Assert.True(counts[1] >= increment2);
        }

        [Fact]
        public async Task IncrByMultipleItemsAsync()
        {
            const int increment1 = 10;
            const int increment2 = 42;

            var keyName = KeyName();

            await _client.InitByDimAsync(keyName, 2000, 5);

            var counts = await _client.IncrByAsync(keyName,
                new Dictionary<string, long> {{"foo", increment1}, {"bar", increment2}});

            Assert.True(counts[0] >= increment1);
            Assert.True(counts[1] >= increment2);
        }

        [Fact]
        public void QueryKeyNotExist()
        {
            Assert.Throws<RedisServerException>(() => _client.Query(KeyName(), "foo"));
        }

        [Fact]
        public async Task QueryKeyNotExistAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() => _client.QueryAsync(KeyName(), "foo"));
        }

        [Fact]
        public void QueryItemNotExist()
        {
            var keyName = KeyName();

            _client.InitByDim(keyName, 20, 5);

            var counts = _client.Query(KeyName(), "foo");

            Assert.Equal(1L, counts.Length);
            Assert.Equal(default, counts[0]);
        }

        [Fact]
        public async Task QueryItemNotExistAsync()
        {
            var keyName = KeyName();

            await _client.InitByDimAsync(keyName, 20, 5);

            var counts = await _client.QueryAsync(KeyName(), "foo");

            Assert.Equal(1L, counts.Length);
            Assert.Equal(default, counts[0]);
        }

        [Fact]
        public void QueryKeyAndItemExists()
        {
            var keyName = KeyName();
            _client.InitByDim(keyName, 20, 5);

            const int increment = 42;

            _client.IncrBy(keyName, "foo", increment);

            var counts = _client.Query(KeyName(), "foo");

            Assert.Equal(1L, counts.Length);
            Assert.Equal(increment, counts[0]);
        }

        [Fact]
        public async Task QueryKeyAndItemExistsAsync()
        {
            var keyName = KeyName();
            await _client.InitByDimAsync(keyName, 20, 5);

            const int increment = 42;

            await _client.IncrByAsync(keyName, "foo", increment);

            var counts = await _client.QueryAsync(KeyName(), "foo");

            Assert.Equal(1L, counts.Length);
            Assert.Equal(increment, counts[0]);
        }

        [Fact]
        public void QueryKeyAndMultipleItemExists()
        {
            var keyName = KeyName();
            _client.InitByDim(keyName, 20, 5);

            const int increment1 = 42;
            const int increment2 = 55;

            _client.IncrBy(keyName, "foo", increment1);
            _client.IncrBy(keyName, "bar", increment2);

            var counts = _client.Query(KeyName(), "foo", "bar");

            Assert.Equal(2L, counts.Length);
            Assert.Equal(increment1, counts[0]);
            Assert.Equal(increment2, counts[1]);
        }

        [Fact]
        public async Task QueryKeyAndMultipleItemExistsAsync()
        {
            var keyName = KeyName();
            await _client.InitByDimAsync(keyName, 20, 5);

            const int increment1 = 42;
            const int increment2 = 55;

            await _client.IncrByAsync(keyName, "foo", increment1);
            await _client.IncrByAsync(keyName, "bar", increment2);

            var counts = await _client.QueryAsync(KeyName(), "foo", "bar");

            Assert.Equal(2L, counts.Length);
            Assert.Equal(increment1, counts[0]);
            Assert.Equal(increment2, counts[1]);
        }

        [Fact]
        public void MergeBasic()
        {
            const int width = 20;
            const int depth = 5;
            const int countOfFoo = 10;
            const int countOfBar = 42;

            var sourceKey = KeyName();
            _client.InitByDim(sourceKey, width, depth);
            _client.IncrBy(sourceKey, "foo", countOfFoo);

            var sourceKey2 = sourceKey + "2";
            _client.InitByDim(sourceKey2, width, depth);
            _client.IncrBy(sourceKey2, "bar", countOfBar);

            var destinationKey = sourceKey + "_new";
            _client.InitByDim(destinationKey, width, depth);

            Assert.True(_client.Merge(destinationKey, sourceKey, sourceKey2));

            var counts = _client.Query(destinationKey, "foo", "bar");

            Assert.Equal(2L, counts.Length);
            Assert.Equal(countOfFoo, counts[0]);
            Assert.Equal(countOfBar, counts[1]);
        }

        [Fact]
        public async Task MergeBasicAsync()
        {
            const int width = 20;
            const int depth = 5;
            const int countOfFoo = 10;
            const int countOfBar = 42;

            var sourceKey = KeyName();
            await _client.InitByDimAsync(sourceKey, width, depth);
            await _client.IncrByAsync(sourceKey, "foo", countOfFoo);

            var sourceKey2 = sourceKey + "2";
            await _client.InitByDimAsync(sourceKey2, width, depth);
            await _client.IncrByAsync(sourceKey2, "bar", countOfBar);

            var destinationKey = sourceKey + "_new";
            await _client.InitByDimAsync(destinationKey, width, depth);

            Assert.True(await _client.MergeAsync(destinationKey, sourceKey, sourceKey2));

            var counts = await _client.QueryAsync(destinationKey, "foo", "bar");

            Assert.Equal(2L, counts.Length);
            Assert.Equal(countOfFoo, counts[0]);
            Assert.Equal(countOfBar, counts[1]);
        }

        [Fact]
        public void InfoKeyExists()
        {
            var keyName = KeyName();

            _client.InitByProb(keyName, 0.001, 0.01);
            _client.IncrBy(keyName, "foo", 42);

            var sketchInfo = _client.Info(keyName);
            Assert.Equal(42, sketchInfo.Count);
            Assert.True(sketchInfo.Depth > 0);
            Assert.True(sketchInfo.Width > 0);
        }

        [Fact]
        public async Task InfoKeyExistsAsync()
        {
            var keyName = KeyName();

            await _client.InitByProbAsync(keyName, 0.001, 0.01);
            await _client.IncrByAsync(keyName, "foo", 42);

            var sketchInfo = await _client.InfoAsync(keyName);
            Assert.Equal(42, sketchInfo.Count);
            Assert.True(sketchInfo.Depth > 0);
            Assert.True(sketchInfo.Width > 0);
        }

        [Fact]
        public void InfoKeyNotExists()
        {
            Assert.Throws<RedisServerException>(() => _client.Info(KeyName()));
        }

        [Fact]
        public async Task InfoKeyNotExistsAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() => _client.InfoAsync(KeyName()));
        }

        private string KeyName([CallerMemberName] string memberName = "")
        {
            return $"{nameof(CountMinSketchClientTests)}_{memberName}";
        }
    }
}
