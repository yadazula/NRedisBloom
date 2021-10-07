using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using NRedisBloom.BloomFilter;
using StackExchange.Redis;
using Xunit;

namespace NRedisBloom.Tests
{
    public class BloomFilterClientTests : TestBase
    {
        private readonly BloomFilterClient _bloomFilter;

        public BloomFilterClientTests()
        {
            _bloomFilter = new BloomFilterClient(Db);
        }

        [Fact]
        public void ReserveBasic()
        {
            var filterName = FilterName();

            Assert.True(_bloomFilter.Reserve(filterName, 100, 0.001));
            Assert.True(_bloomFilter.Add(filterName, "val1"));
            Assert.True(_bloomFilter.Exists(filterName, "val1"));
            Assert.False(_bloomFilter.Exists(filterName, "val2"));
        }

        [Fact]
        public async Task ReserveBasicAsync()
        {
            var filterName = FilterName();

            Assert.True(await _bloomFilter.ReserveAsync(filterName, 100, 0.001));
            Assert.True(await _bloomFilter.AddAsync(filterName, "val1"));
            Assert.True(await _bloomFilter.ExistsAsync(filterName, "val1"));
            Assert.False(await _bloomFilter.ExistsAsync(filterName, "val2"));
        }

        [Fact]
        public void ReserveValidateZeroCapacity()
        {
            Assert.Throws<RedisServerException>(() =>
                _bloomFilter.Reserve(FilterName(), 0, 0.001));
        }

        [Fact]
        public async Task ReserveValidateZeroCapacityAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() =>
                _bloomFilter.ReserveAsync(FilterName(), 0, 0.001));
        }

        [Fact]
        public void ReserveValidateZeroError()
        {
            Assert.Throws<RedisServerException>(() =>
                _bloomFilter.Reserve(FilterName(), 100, 0));
        }

        [Fact]
        public async Task ReserveValidateZeroErrorAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() =>
                _bloomFilter.ReserveAsync(FilterName(), 100, 0));
        }

        [Fact]
        public void ReserveAlreadyExists()
        {
            var filterName = FilterName();

            Assert.True(_bloomFilter.Reserve(filterName, 100, 0.1));

            Assert.Throws<RedisServerException>(() => _bloomFilter.Reserve(filterName, 100, 0.1));
        }

        [Fact]
        public async Task ReserveAlreadyExistsAsync()
        {
            var filterName = FilterName();

            Assert.True(await _bloomFilter.ReserveAsync(filterName, 100, 0.1));

            await Assert.ThrowsAsync<RedisServerException>(() => _bloomFilter.ReserveAsync(filterName, 100, 0.1));
        }

        [Fact]
        public void ReserveNonScalingExpand()
        {
            Assert.Throws<RedisServerException>(() =>
                _bloomFilter.Reserve(FilterName(), 10, 0.01, expansion: 2, nonScaling: true));
        }

        [Fact]
        public async Task ReserveNonScalingExpandAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() =>
                _bloomFilter.ReserveAsync(FilterName(), 10, 0.01, expansion: 2,
                    nonScaling: true));
        }

        [Fact]
        public void InsertBasic()
        {
            var insertOptions = new InsertOptions
            {
                Capacity = 1000,
                Expansion = 4,
                ErrorRate = 0.01,
                NoCreate = false,
                NonScaling = true
            };

            var result = _bloomFilter.Insert(FilterName(), insertOptions, "foo", "bar");

            Assert.Equal(2, result.Length);
            foreach (var item in result)
            {
                Assert.True(item);
            }
        }

        [Fact]
        public void InsertNoCreateFilterNotExist()
        {
            var insertOptions = new InsertOptions { NoCreate = true };

            Assert.Throws<RedisServerException>(() =>
                _bloomFilter.Insert(FilterName(), insertOptions, "foo"));
        }

        [Fact]
        public async Task InsertBasicAsync()
        {
            var insertOptions = new InsertOptions
            {
                Capacity = 1000,
                Expansion = 4,
                ErrorRate = 0.01,
                NoCreate = false,
                NonScaling = true
            };

            var result = await _bloomFilter.InsertAsync(FilterName(), insertOptions, "foo", "bar");

            Assert.Equal(2, result.Length);
            foreach (var item in result)
            {
                Assert.True(item);
            }
        }

        [Fact]
        public async Task InsertNoCreateFilterNotExistAsync()
        {
            var insertOptions = new InsertOptions { NoCreate = true };

            await Assert.ThrowsAsync<RedisServerException>(() =>
                _bloomFilter.InsertAsync(FilterName(), insertOptions, "foo"));
        }

        [Fact]
        public void AddExist()
        {
            var filterName = FilterName();

            Assert.True(_bloomFilter.Add(filterName, "foo"));
            Assert.True(_bloomFilter.Exists(filterName, "foo"));
            Assert.False(_bloomFilter.Exists(filterName, "bar"));
            Assert.False(_bloomFilter.Add(filterName, "foo"));
        }

        [Fact]
        public async Task AddExistAsync()
        {
            var filterName = FilterName();

            Assert.True(await _bloomFilter.AddAsync(filterName, "foo"));
            Assert.True(await _bloomFilter.ExistsAsync(filterName, "foo"));
            Assert.False(await _bloomFilter.ExistsAsync(filterName, "bar"));
            Assert.False(await _bloomFilter.AddAsync(filterName, "foo"));
        }

        [Fact]
        public void TestExistsNonExist()
        {
            Assert.False(_bloomFilter.Exists(FilterName(), "foo"));
        }

        [Fact]
        public async Task TestExistsNonExistAsync()
        {
            Assert.False(await _bloomFilter.ExistsAsync(FilterName(), "foo"));
        }

        [Fact]
        public void AddExistsMulti()
        {
            var filterName = FilterName();

            var result = _bloomFilter.AddMultiple(filterName, "foo", "bar", "baz");
            Assert.Equal(3, result.Length);
            foreach (var item in result)
            {
                Assert.True(item);
            }

            result = _bloomFilter.ExistsMultiple(filterName, "foo", "bar", "baz");
            Assert.Equal(3, result.Length);
            foreach (var item in result)
            {
                Assert.True(item);
            }

            result = _bloomFilter.AddMultiple(filterName, "newElem", "bar", "baz");
            Assert.Equal(3, result.Length);
            Assert.True(result[0]);
            Assert.False(result[1]);
            Assert.False(result[2]);

            result = _bloomFilter.ExistsMultiple(filterName, "foo", "notExist", "bar");
            Assert.Equal(3, result.Length);
            Assert.True(result[0]);
            Assert.False(result[1]);
            Assert.True(result[2]);
        }

        [Fact]
        public async Task AddExistsMultiAsync()
        {
            var filterName = FilterName();

            var result = await _bloomFilter.AddMultipleAsync(filterName, "foo", "bar", "baz");
            Assert.Equal(3, result.Length);
            foreach (var item in result)
            {
                Assert.True(item);
            }

            result = await _bloomFilter.ExistsMultipleAsync(filterName, "foo", "bar", "baz");
            Assert.Equal(3, result.Length);
            foreach (var item in result)
            {
                Assert.True(item);
            }

            result = await _bloomFilter.AddMultipleAsync(filterName, "newElem", "bar", "baz");
            Assert.Equal(3, result.Length);
            Assert.True(result[0]);
            Assert.False(result[1]);
            Assert.False(result[2]);

            result = await _bloomFilter.ExistsMultipleAsync(filterName, "foo", "notExist", "bar");
            Assert.Equal(3, result.Length);
            Assert.True(result[0]);
            Assert.False(result[1]);
            Assert.True(result[2]);
        }

        [Fact]
        public void InfoFilterExist()
        {
            var filterName = FilterName();

            const int capacity = 100;
            const int expansion = 2;

            _bloomFilter.Reserve(filterName, capacity, 0.01, expansion: expansion);

            var filterInfo = _bloomFilter.Info(filterName);
            Assert.NotNull(filterInfo);
            Assert.Equal(capacity, filterInfo.Capacity);
            Assert.True(filterInfo.Size > 0);
            Assert.Equal(1, filterInfo.NumberOfFilters);
            Assert.Equal(0, filterInfo.NumberOfItemsInserted);
            Assert.Equal(expansion, filterInfo.ExpansionRate);
        }

        [Fact]
        public async Task InfoFilterExistAsync()
        {
            var filterName = FilterName();

            const int capacity = 100;
            const int expansion = 2;

            await _bloomFilter.ReserveAsync(filterName, capacity, 0.01, expansion: expansion);

            var filterInfo = await _bloomFilter.InfoAsync(filterName);
            Assert.NotNull(filterInfo);
            Assert.Equal(capacity, filterInfo.Capacity);
            Assert.True(filterInfo.Size > 0);
            Assert.Equal(1, filterInfo.NumberOfFilters);
            Assert.Equal(0, filterInfo.NumberOfItemsInserted);
            Assert.Equal(expansion, filterInfo.ExpansionRate);
        }

        [Fact]
        public void InfoFilterNotExist()
        {
            Assert.Throws<RedisServerException>(() => _bloomFilter.Info(FilterName()));
        }

        [Fact]
        public async Task InfoFilterNotExistAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() => _bloomFilter.InfoAsync(FilterName()));
        }

        [Fact]
        public void ScanDumpAndLoadBasic()
        {
            var existingFilter = FilterName();
            var newFilter = existingFilter + "New";

            _bloomFilter.AddMultiple(existingFilter, "foo", "bar");

            var iterator = 0L;
            while (true)
            {
                var scanDump = _bloomFilter.ScanDump(existingFilter, iterator);
                iterator = scanDump.Iterator;

                if (iterator == 0)
                {
                    break;
                }

                Assert.True(scanDump.Data.Length > 0);

                var loadResult = _bloomFilter.LoadChunk(newFilter, iterator, scanDump.Data);
                Assert.True(loadResult);
            }
        }

        [Fact]
        public async Task ScanDumpAndLoadBasicAsync()
        {
            var existingFilter = FilterName();
            var newFilter = existingFilter + "New";

            await _bloomFilter.AddMultipleAsync(existingFilter, "foo", "bar", "baz");

            var iterator = 0L;
            while (true)
            {
                var scanDump = await _bloomFilter.ScanDumpAsync(existingFilter, iterator);
                iterator = scanDump.Iterator;

                if (iterator == 0)
                {
                    break;
                }

                Assert.True(scanDump.Data.Length > 0);

                var loadResult = await _bloomFilter.LoadChunkAsync(newFilter, iterator, scanDump.Data);
                Assert.True(loadResult);
            }
        }

        private string FilterName([CallerMemberName] string memberName = "")
        {
            return $"{nameof(BloomFilterClientTests)}_{memberName}";
        }
    }
}
