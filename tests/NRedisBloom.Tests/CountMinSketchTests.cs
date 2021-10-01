using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using NRedisBloom.CountMinSketch;
using StackExchange.Redis;
using Xunit;

namespace NRedisBloom.Tests
{
    public class CountMinSketchTests : TestBase
    {
        [Fact]
        public void InitByDimBasic()
        {
            const int width = 2000;
            const int depth = 5;

            var keyName = KeyName();

            Assert.True(Db.CountMinSketchInitByDim(keyName, width, depth));

            var sketchInfo = Db.CountMinSketchInfo(keyName);
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

            Assert.True(await Db.CountMinSketchInitByDimAsync(keyName, width, depth));

            var sketchInfo = await Db.CountMinSketchInfoAsync(keyName);
            Assert.Equal(width, sketchInfo.Width);
            Assert.Equal(depth, sketchInfo.Depth);
            Assert.Equal(default, sketchInfo.Count);
        }

        [Fact]
        public void InitByProbBasic()
        {
            var keyName = KeyName();

            Assert.True(Db.CountMinSketchInitByProb(keyName, 0.001, 0.01));

            var sketchInfo = Db.CountMinSketchInfo(keyName);
            Assert.Equal(default, sketchInfo.Count);
        }

        [Fact]
        public async Task InitByProbBasicAsync()
        {
            var keyName = KeyName();

            Assert.True(await Db.CountMinSketchInitByProbAsync(keyName, 0.001, 0.01));

            var sketchInfo = await Db.CountMinSketchInfoAsync(keyName);
            Assert.Equal(default, sketchInfo.Count);
        }

        [Fact]
        public void IncrBySingleItem()
        {
            const int increment = 10;

            var keyName = KeyName();

            Db.CountMinSketchInitByDim(keyName, 2000, 5);

            var count = Db.CountMinSketchIncrBy(keyName, "foo", increment);

            Assert.True(count >= increment);
        }

        [Fact]
        public async Task IncrBySingleItemAsync()
        {
            const int increment = 10;

            var keyName = KeyName();

            await Db.CountMinSketchInitByDimAsync(keyName, 2000, 5);

            var count = await Db.CountMinSketchIncrByAsync(keyName, "foo", increment);

            Assert.True(count >= increment);
        }

        [Fact]
        public void IncrByMultipleItems()
        {
            const int increment1 = 10;
            const int increment2 = 42;

            var keyName = KeyName();

            Db.CountMinSketchInitByDim(keyName, 2000, 5);

            var counts = Db.CountMinSketchIncrBy(keyName,
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

            await Db.CountMinSketchInitByDimAsync(keyName, 2000, 5);

            var counts = await Db.CountMinSketchIncrByAsync(keyName,
                new Dictionary<string, long> {{"foo", increment1}, {"bar", increment2}});

            Assert.True(counts[0] >= increment1);
            Assert.True(counts[1] >= increment2);
        }

        [Fact]
        public void QueryKeyNotExist()
        {
            Assert.Throws<RedisServerException>(() => Db.CountMinSketchQuery(KeyName(), "foo"));
        }

        [Fact]
        public async Task QueryKeyNotExistAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() => Db.CountMinSketchQueryAsync(KeyName(), "foo"));
        }

        [Fact]
        public void QueryItemNotExist()
        {
            var keyName = KeyName();

            Db.CountMinSketchInitByDim(keyName, 20, 5);

            var counts = Db.CountMinSketchQuery(KeyName(), "foo");

            Assert.Equal(1L, counts.Length);
            Assert.Equal(default, counts[0]);
        }

        [Fact]
        public async Task QueryItemNotExistAsync()
        {
            var keyName = KeyName();

            await Db.CountMinSketchInitByDimAsync(keyName, 20, 5);

            var counts = await Db.CountMinSketchQueryAsync(KeyName(), "foo");

            Assert.Equal(1L, counts.Length);
            Assert.Equal(default, counts[0]);
        }

        [Fact]
        public void QueryKeyAndItemExists()
        {
            var keyName = KeyName();
            Db.CountMinSketchInitByDim(keyName, 20, 5);

            const int increment = 42;

            Db.CountMinSketchIncrBy(keyName, "foo", increment);

            var counts = Db.CountMinSketchQuery(KeyName(), "foo");

            Assert.Equal(1L, counts.Length);
            Assert.Equal(increment, counts[0]);
        }

        [Fact]
        public async Task QueryKeyAndItemExistsAsync()
        {
            var keyName = KeyName();
            await Db.CountMinSketchInitByDimAsync(keyName, 20, 5);

            const int increment = 42;

            await Db.CountMinSketchIncrByAsync(keyName, "foo", increment);

            var counts = await Db.CountMinSketchQueryAsync(KeyName(), "foo");

            Assert.Equal(1L, counts.Length);
            Assert.Equal(increment, counts[0]);
        }

        [Fact]
        public void QueryKeyAndMultipleItemExists()
        {
            var keyName = KeyName();
            Db.CountMinSketchInitByDim(keyName, 20, 5);

            const int increment1 = 42;
            const int increment2 = 55;

            Db.CountMinSketchIncrBy(keyName, "foo", increment1);
            Db.CountMinSketchIncrBy(keyName, "bar", increment2);

            var counts = Db.CountMinSketchQuery(KeyName(), "foo", "bar");

            Assert.Equal(2L, counts.Length);
            Assert.Equal(increment1, counts[0]);
            Assert.Equal(increment2, counts[1]);
        }

        [Fact]
        public async Task QueryKeyAndMultipleItemExistsAsync()
        {
            var keyName = KeyName();
            await Db.CountMinSketchInitByDimAsync(keyName, 20, 5);

            const int increment1 = 42;
            const int increment2 = 55;

            await Db.CountMinSketchIncrByAsync(keyName, "foo", increment1);
            await Db.CountMinSketchIncrByAsync(keyName, "bar", increment2);

            var counts = await Db.CountMinSketchQueryAsync(KeyName(), "foo", "bar");

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
            Db.CountMinSketchInitByDim(sourceKey, width, depth);
            Db.CountMinSketchIncrBy(sourceKey, "foo", countOfFoo);

            var sourceKey2 = sourceKey + "2";
            Db.CountMinSketchInitByDim(sourceKey2, width, depth);
            Db.CountMinSketchIncrBy(sourceKey2, "bar", countOfBar);

            var destinationKey = sourceKey + "_new";
            Db.CountMinSketchInitByDim(destinationKey, width, depth);

            Assert.True(Db.CountMinSketchMerge(destinationKey, sourceKey, sourceKey2));

            var counts = Db.CountMinSketchQuery(destinationKey, "foo", "bar");

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
            await Db.CountMinSketchInitByDimAsync(sourceKey, width, depth);
            await Db.CountMinSketchIncrByAsync(sourceKey, "foo", countOfFoo);

            var sourceKey2 = sourceKey + "2";
            await Db.CountMinSketchInitByDimAsync(sourceKey2, width, depth);
            await Db.CountMinSketchIncrByAsync(sourceKey2, "bar", countOfBar);

            var destinationKey = sourceKey + "_new";
            await Db.CountMinSketchInitByDimAsync(destinationKey, width, depth);

            Assert.True(await Db.CountMinSketchMergeAsync(destinationKey, sourceKey, sourceKey2));

            var counts = await Db.CountMinSketchQueryAsync(destinationKey, "foo", "bar");

            Assert.Equal(2L, counts.Length);
            Assert.Equal(countOfFoo, counts[0]);
            Assert.Equal(countOfBar, counts[1]);
        }

        [Fact]
        public void InfoKeyExists()
        {
            var keyName = KeyName();

            Db.CountMinSketchInitByProb(keyName, 0.001, 0.01);
            Db.CountMinSketchIncrBy(keyName, "foo", 42);

            var sketchInfo = Db.CountMinSketchInfo(keyName);
            Assert.Equal(42, sketchInfo.Count);
            Assert.True(sketchInfo.Depth > 0);
            Assert.True(sketchInfo.Width > 0);
        }

        [Fact]
        public async Task InfoKeyExistsAsync()
        {
            var keyName = KeyName();

            await Db.CountMinSketchInitByProbAsync(keyName, 0.001, 0.01);
            await Db.CountMinSketchIncrByAsync(keyName, "foo", 42);

            var sketchInfo = await Db.CountMinSketchInfoAsync(keyName);
            Assert.Equal(42, sketchInfo.Count);
            Assert.True(sketchInfo.Depth > 0);
            Assert.True(sketchInfo.Width > 0);
        }

        [Fact]
        public void InfoKeyNotExists()
        {
            Assert.Throws<RedisServerException>(() => Db.CountMinSketchInfo(KeyName()));
        }

        [Fact]
        public async Task InfoKeyNotExistsAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() => Db.CountMinSketchInfoAsync(KeyName()));
        }

        private string KeyName([CallerMemberName] string memberName = "")
        {
            return $"{nameof(CountMinSketchTests)}_{memberName}";
        }
    }
}
