using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using NRedisBloom.TopK;
using StackExchange.Redis;
using Xunit;

namespace NRedisBloom.Tests
{
    public class TopKTests : TestBase
    {
        [Fact]
        public void ReserveBasic()
        {
            Assert.True(Db.TopKReserve(FilterName(), 50, 2000, 7, 0.925));
        }

        [Fact]
        public async Task ReserveBasicAsync()
        {
            Assert.True(await Db.TopKReserveAsync(FilterName(), 50, 2000, 7, 0.925));
        }

        [Fact]
        public void AddBasic()
        {
            var filterName = FilterName();

            Assert.True(Db.TopKReserve(filterName, 50));

            var droppedItems = Db.TopKAdd(filterName, "foo", "bar");

            foreach (var droppedItem in droppedItems)
            {
                Assert.Null(droppedItem);
            }
        }

        [Fact]
        public async Task AddBasicAsync()
        {
            var filterName = FilterName();

            Assert.True(await Db.TopKReserveAsync(filterName, 50));

            var droppedItems = await Db.TopKAddAsync(filterName, "foo", "bar");

            foreach (var droppedItem in droppedItems)
            {
                Assert.Null(droppedItem);
            }
        }

        [Fact]
        public void AddFilterNotExist()
        {
            Assert.Throws<RedisServerException>(() => Db.TopKAdd(FilterName(), "foo"));
        }

        [Fact]
        public async Task AddFilterNotExistAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() => Db.TopKAddAsync(FilterName(), "foo"));
        }

        [Fact]
        public void IncrementByBasic()
        {
            var filterName = FilterName();

            Db.TopKReserve(filterName, 50);

            Db.TopKAdd(filterName, "foo", "bar");

            var droppedItem = Db.TopKIncrementBy(filterName, "foo", 3);

            Assert.Null(droppedItem);
        }

        [Fact]
        public async Task IncrementByBasicAsync()
        {
            var filterName = FilterName();

            await Db.TopKReserveAsync(filterName, 50);

            await Db.TopKAddAsync(filterName, "foo", "bar");

            var droppedItem = await Db.TopKIncrementByAsync(filterName, "foo", 3);

            Assert.Null(droppedItem);
        }

        [Fact]
        public void QueryBasic()
        {
            var filterName = FilterName();

            Db.TopKReserve(filterName, 50);

            Db.TopKAdd(filterName, "foo", "bar");

            var result = Db.TopKQuery(filterName, "foo", "nonexist");

            Assert.Equal(2, result.Length);
            Assert.True(result[0]);
            Assert.False(result[1]);
        }

        [Fact]
        public async Task QueryBasicAsync()
        {
            var filterName = FilterName();

            await Db.TopKReserveAsync(filterName, 50);

            await Db.TopKAddAsync(filterName, "foo", "bar");

            var result = await Db.TopKQueryAsync(filterName, "foo", "nonexist");

            Assert.Equal(2, result.Length);
            Assert.True(result[0]);
            Assert.False(result[1]);
        }

        [Fact]
        public void CountBasic()
        {
            var filterName = FilterName();

            Db.TopKReserve(filterName, 50);

            Db.TopKAdd(filterName, "foo");

            Db.TopKIncrementBy(filterName, "foo", 2);

            var result = Db.TopKCount(filterName, "foo", "nonexist");

            Assert.Equal(2, result.Length);
            Assert.Equal(3, result[0]);
            Assert.Equal(0, result[1]);
        }

        [Fact]
        public async Task CountBasicAsync()
        {
            var filterName = FilterName();

            await Db.TopKReserveAsync(filterName, 50);

            await Db.TopKAddAsync(filterName, "foo");

            await Db.TopKIncrementByAsync(filterName, "foo", 2);

            var result = await Db.TopKCountAsync(filterName, "foo", "nonexist");

            Assert.Equal(2, result.Length);
            Assert.Equal(3, result[0]);
            Assert.Equal(0, result[1]);
        }

        [Fact]
        public void ListBasic()
        {
            var filterName = FilterName();

            Db.TopKReserve(filterName, 50);

            Db.TopKAdd(filterName, "foo", "bar");

            var result = Db.TopKList(filterName);

            Assert.Equal(50, result.Length);
            Assert.Contains("foo", result);
            Assert.Contains("bar", result);
        }

        [Fact]
        public async Task ListBasicAsync()
        {
            var filterName = FilterName();

            await Db.TopKReserveAsync(filterName, 50);

            await Db.TopKAddAsync(filterName, "foo", "bar");

            var result = await Db.TopKListAsync(filterName);

            Assert.Equal(50, result.Length);
            Assert.Contains("foo", result);
            Assert.Contains("bar", result);
        }

        [Fact]
        public void InfoFilterExist()
        {
            var filterName = FilterName();

            const long topk = 50;
            const long width = 2000;
            const long depth = 7;
            const double decay = 0.925;

            Db.TopKReserve(filterName, topk, width, depth, decay);

            var topKInfo = Db.TopKInfo(filterName);

            Assert.Equal(topk, topKInfo.K);
            Assert.Equal(width, topKInfo.Width);
            Assert.Equal(depth, topKInfo.Depth);
            Assert.Equal(decay, topKInfo.Decay);
        }

        [Fact]
        public async Task InfoFilterExistAsync()
        {
            var filterName = FilterName();

            const long topk = 50;
            const long width = 2000;
            const long depth = 7;
            const double decay = 0.925;

            await Db.TopKReserveAsync(filterName, topk, width, depth, decay);

            var topKInfo = await Db.TopKInfoAsync(filterName);

            Assert.Equal(topk, topKInfo.K);
            Assert.Equal(width, topKInfo.Width);
            Assert.Equal(depth, topKInfo.Depth);
            Assert.Equal(decay, topKInfo.Decay);
        }

        [Fact]
        public void InfoFilterNotExist()
        {
            Assert.Throws<RedisServerException>(() => Db.TopKInfo(FilterName()));
        }

        [Fact]
        public async Task InfoFilterNotExistAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() => Db.TopKInfoAsync(FilterName()));
        }

        private string FilterName([CallerMemberName] string memberName = "")
        {
            return $"{nameof(TopKTests)}_{memberName}";
        }
    }
}
