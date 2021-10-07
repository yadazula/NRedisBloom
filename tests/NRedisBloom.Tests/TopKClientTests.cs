using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using NRedisBloom.TopK;
using StackExchange.Redis;
using Xunit;

namespace NRedisBloom.Tests
{
    public class TopKClientTests : TestBase
    {
        private readonly TopKClient _topk;

        public TopKClientTests()
        {
            _topk = new TopKClient(Db);
        }

        [Fact]
        public void ReserveBasic()
        {
            Assert.True(_topk.Reserve(FilterName(), 50, 2000, 7, 0.925));
        }

        [Fact]
        public async Task ReserveBasicAsync()
        {
            Assert.True(await _topk.ReserveAsync(FilterName(), 50, 2000, 7, 0.925));
        }

        [Fact]
        public void AddBasic()
        {
            var filterName = FilterName();

            Assert.True(_topk.Reserve(filterName, 50));

            var droppedItems = _topk.Add(filterName, "foo", "bar");

            foreach (var droppedItem in droppedItems)
            {
                Assert.Null(droppedItem);
            }
        }

        [Fact]
        public async Task AddBasicAsync()
        {
            var filterName = FilterName();

            Assert.True(await _topk.ReserveAsync(filterName, 50));

            var droppedItems = await _topk.AddAsync(filterName, "foo", "bar");

            foreach (var droppedItem in droppedItems)
            {
                Assert.Null(droppedItem);
            }
        }

        [Fact]
        public void AddFilterNotExist()
        {
            Assert.Throws<RedisServerException>(() => _topk.Add(FilterName(), "foo"));
        }

        [Fact]
        public async Task AddFilterNotExistAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() => _topk.AddAsync(FilterName(), "foo"));
        }

        [Fact]
        public void IncrementByBasic()
        {
            var filterName = FilterName();

            _topk.Reserve(filterName, 50);

            _topk.Add(filterName, "foo", "bar");

            var droppedItem = _topk.IncrementBy(filterName, "foo", 3);

            Assert.Null(droppedItem);
        }

        [Fact]
        public async Task IncrementByBasicAsync()
        {
            var filterName = FilterName();

            await _topk.ReserveAsync(filterName, 50);

            await _topk.AddAsync(filterName, "foo", "bar");

            var droppedItem = await _topk.IncrementByAsync(filterName, "foo", 3);

            Assert.Null(droppedItem);
        }

        [Fact]
        public void QueryBasic()
        {
            var filterName = FilterName();

            _topk.Reserve(filterName, 50);

            _topk.Add(filterName, "foo", "bar");

            var result = _topk.Query(filterName, "foo", "nonexist");

            Assert.Equal(2, result.Length);
            Assert.True(result[0]);
            Assert.False(result[1]);
        }

        [Fact]
        public async Task QueryBasicAsync()
        {
            var filterName = FilterName();

            await _topk.ReserveAsync(filterName, 50);

            await _topk.AddAsync(filterName, "foo", "bar");

            var result = await _topk.QueryAsync(filterName, "foo", "nonexist");

            Assert.Equal(2, result.Length);
            Assert.True(result[0]);
            Assert.False(result[1]);
        }

        [Fact]
        public void CountBasic()
        {
            var filterName = FilterName();

            _topk.Reserve(filterName, 50);

            _topk.Add(filterName, "foo");

            _topk.IncrementBy(filterName, "foo", 2);

            var result = _topk.Count(filterName, "foo", "nonexist");

            Assert.Equal(2, result.Length);
            Assert.Equal(3, result[0]);
            Assert.Equal(0, result[1]);
        }

        [Fact]
        public async Task CountBasicAsync()
        {
            var filterName = FilterName();

            await _topk.ReserveAsync(filterName, 50);

            await _topk.AddAsync(filterName, "foo");

            await _topk.IncrementByAsync(filterName, "foo", 2);

            var result = await _topk.CountAsync(filterName, "foo", "nonexist");

            Assert.Equal(2, result.Length);
            Assert.Equal(3, result[0]);
            Assert.Equal(0, result[1]);
        }

        [Fact]
        public void ListBasic()
        {
            var filterName = FilterName();

            _topk.Reserve(filterName, 50);

            _topk.Add(filterName, "foo", "bar");

            var result = _topk.List(filterName);

            Assert.Equal(50, result.Length);
            Assert.Contains("foo", result);
            Assert.Contains("bar", result);
        }

        [Fact]
        public async Task ListBasicAsync()
        {
            var filterName = FilterName();

            await _topk.ReserveAsync(filterName, 50);

            await _topk.AddAsync(filterName, "foo", "bar");

            var result = await _topk.ListAsync(filterName);

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

            _topk.Reserve(filterName, topk, width, depth, decay);

            var topKInfo = _topk.Info(filterName);

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

            await _topk.ReserveAsync(filterName, topk, width, depth, decay);

            var topKInfo = await _topk.InfoAsync(filterName);

            Assert.Equal(topk, topKInfo.K);
            Assert.Equal(width, topKInfo.Width);
            Assert.Equal(depth, topKInfo.Depth);
            Assert.Equal(decay, topKInfo.Decay);
        }

        [Fact]
        public void InfoFilterNotExist()
        {
            Assert.Throws<RedisServerException>(() => _topk.Info(FilterName()));
        }

        [Fact]
        public async Task InfoFilterNotExistAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() => _topk.InfoAsync(FilterName()));
        }

        private string FilterName([CallerMemberName] string memberName = "")
        {
            return $"{nameof(TopKClientTests)}_{memberName}";
        }
    }
}
