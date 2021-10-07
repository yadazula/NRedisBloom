using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using NRedisBloom.CuckooFilter;
using StackExchange.Redis;
using Xunit;

namespace NRedisBloom.Tests
{
    public class CuckooFilterClientTests : TestBase
    {
        private readonly CuckooFilterClient _cuckooFilter;

        public CuckooFilterClientTests()
        {
            _cuckooFilter = new CuckooFilterClient(Db);
        }

        [Fact]
        public void ReserveBasic()
        {
            Assert.True(_cuckooFilter.Reserve(FilterName(), 100));
        }

        [Fact]
        public async Task ReserveBasicAsync()
        {
            Assert.True(await _cuckooFilter.ReserveAsync(FilterName(), 100));
        }

        [Fact]
        public void ReserveWithOptionalParameters()
        {
            Assert.True(_cuckooFilter.Reserve(FilterName(), 100, 8, 20, 2));
        }

        [Fact]
        public async Task ReserveWithOptionalParametersAsync()
        {
            Assert.True(await _cuckooFilter.ReserveAsync(FilterName(), 100, 8, 20, 2));
        }

        [Fact]
        public void AddBasic()
        {
            Assert.True(_cuckooFilter.Add(FilterName(), "foo"));
        }

        [Fact]
        public async Task AddBasicAsync()
        {
            Assert.True(await _cuckooFilter.AddAsync(FilterName(), "foo"));
        }

        [Fact]
        public void AddAdvanced()
        {
            Assert.True(_cuckooFilter.AddAdvanced(FilterName(), "foo"));
        }

        [Fact]
        public async Task AddAdvancedAsync()
        {
            Assert.True(await _cuckooFilter.AddAdvancedAsync(FilterName(), "foo"));
        }

        [Fact]
        public void AddAdvancedExists()
        {
            var filterName = FilterName();

            _cuckooFilter.Add(filterName, "foo");

            Assert.False(_cuckooFilter.AddAdvanced(filterName, "foo"));
        }

        [Fact]
        public async Task AddAdvancedExistsAsync()
        {
            var filterName = FilterName();

            await _cuckooFilter.AddAsync(filterName, "foo");

            Assert.False(await _cuckooFilter.AddAdvancedAsync(filterName, "foo"));
        }

        [Fact]
        public void InsertBasic()
        {
            var insertOptions = new InsertOptions { Capacity = 1000, NoCreate = false, };

            var result = _cuckooFilter.Insert(FilterName(), insertOptions, "foo", "bar");

            Assert.Equal(2, result.Length);
            foreach (var item in result)
            {
                Assert.True(item);
            }
        }

        [Fact]
        public async Task InsertBasicAsync()
        {
            var insertOptions = new InsertOptions { Capacity = 1000, NoCreate = false, };

            var result = await _cuckooFilter.InsertAsync(FilterName(), insertOptions, "foo", "bar");

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
                _cuckooFilter.Insert(FilterName(), insertOptions, "foo"));
        }

        [Fact]
        public async Task InsertNoCreateFilterNotExistAsync()
        {
            var insertOptions = new InsertOptions { NoCreate = true };

            await Assert.ThrowsAsync<RedisServerException>(() =>
                _cuckooFilter.InsertAsync(FilterName(), insertOptions, "foo"));
        }

        [Fact]
        public void InsertAdvancedBasic()
        {
            var insertOptions = new InsertOptions { Capacity = 1000, NoCreate = false, };

            var result = _cuckooFilter.InsertAdvanced(FilterName(), insertOptions, "foo", "bar");

            Assert.Equal(2, result.Length);
            foreach (var item in result)
            {
                Assert.True(item);
            }
        }

        [Fact]
        public void InsertAdvancedExist()
        {
            var insertOptions = new InsertOptions { Capacity = 1000, NoCreate = false, };

            _cuckooFilter.Insert(FilterName(), insertOptions, "foo", "bar");

            var result = _cuckooFilter.InsertAdvanced(FilterName(), insertOptions, "foo", "bar");

            Assert.Equal(2, result.Length);
            foreach (var item in result)
            {
                Assert.False(item);
            }
        }

        [Fact]
        public async Task InsertAdvancedBasicAsync()
        {
            var insertOptions = new InsertOptions { Capacity = 1000, NoCreate = false, };

            var result = await _cuckooFilter.InsertAdvancedAsync(FilterName(), insertOptions, "foo", "bar");

            Assert.Equal(2, result.Length);
            foreach (var item in result)
            {
                Assert.True(item);
            }
        }

        [Fact]
        public async Task InsertAdvancedExistAsync()
        {
            var insertOptions = new InsertOptions { Capacity = 1000, NoCreate = false, };

            await _cuckooFilter.InsertAsync(FilterName(), insertOptions, "foo", "bar");

            var result = await _cuckooFilter.InsertAdvancedAsync(FilterName(), insertOptions, "foo", "bar");

            Assert.Equal(2, result.Length);
            foreach (var item in result)
            {
                Assert.False(item);
            }
        }

        [Fact]
        public void ExistsNonExist()
        {
            Assert.False(_cuckooFilter.Exists(FilterName(), "foo"));
        }

        [Fact]
        public async Task ExistsNonExistAsync()
        {
            Assert.False(await _cuckooFilter.ExistsAsync(FilterName(), "foo"));
        }

        [Fact]
        public void AddExist()
        {
            var filterName = FilterName();

            Assert.True(_cuckooFilter.Add(filterName, "foo"));
            Assert.True(_cuckooFilter.Exists(filterName, "foo"));
            Assert.False(_cuckooFilter.Exists(filterName, "bar"));
        }

        [Fact]
        public async Task AddExistAsync()
        {
            var filterName = FilterName();

            Assert.True(await _cuckooFilter.AddAsync(filterName, "foo"));
            Assert.True(await _cuckooFilter.ExistsAsync(filterName, "foo"));
            Assert.False(await _cuckooFilter.ExistsAsync(filterName, "bar"));
        }

        [Fact]
        public void DeleteBasic()
        {
            var filterName = FilterName();

            _cuckooFilter.Add(FilterName(), "foo");
            Assert.True(_cuckooFilter.Delete(FilterName(), "foo"));
        }

        [Fact]
        public async Task DeleteBasicAsync()
        {
            var filterName = FilterName();

            await _cuckooFilter.AddAsync(FilterName(), "foo");
            Assert.True(await _cuckooFilter.DeleteAsync(FilterName(), "foo"));
        }

        [Fact]
        public void DeleteFilterNotExist()
        {
            Assert.Throws<RedisServerException>(() => _cuckooFilter.Delete(FilterName(), "foo"));
        }

        [Fact]
        public void DeleteItemNotExist()
        {
            var filterName = FilterName();

            _cuckooFilter.Reserve(filterName, 100);

            Assert.False(_cuckooFilter.Delete(filterName, "bar"));
        }

        [Fact]
        public async Task DeleteFilterNotExistAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() => _cuckooFilter.DeleteAsync(FilterName(), "foo"));
        }

        [Fact]
        public async Task DeleteItemNotExistAsync()
        {
            var filterName = FilterName();

            await _cuckooFilter.ReserveAsync(filterName, 100);

            Assert.False(await _cuckooFilter.DeleteAsync(filterName, "bar"));
        }

        [Fact]
        public void CountSingle()
        {
            var filterName = FilterName();

            _cuckooFilter.Add(filterName, "foo");

            Assert.Equal(1, _cuckooFilter.Count(filterName, "foo"));
        }

        [Fact]
        public void CountMultiple()
        {
            var filterName = FilterName();

            _cuckooFilter.Add(filterName, "foo");
            _cuckooFilter.Add(filterName, "foo");

            Assert.Equal(2, _cuckooFilter.Count(filterName, "foo"));
        }

        [Fact]
        public async Task CountSingleAsync()
        {
            var filterName = FilterName();

            await _cuckooFilter.AddAsync(filterName, "foo");

            Assert.Equal(1, await _cuckooFilter.CountAsync(filterName, "foo"));
        }

        [Fact]
        public async Task CountMultipleAsync()
        {
            var filterName = FilterName();

            await _cuckooFilter.AddAsync(filterName, "foo");
            await _cuckooFilter.AddAsync(filterName, "foo");

            Assert.Equal(2, await _cuckooFilter.CountAsync(filterName, "foo"));
        }

        [Fact]
        public void ScanDumpAndLoadBasic()
        {
            var existingFilter = FilterName();
            var newFilter = existingFilter + "New";

            _cuckooFilter.Insert(existingFilter, "foo", "bar");

            var iterator = 0L;
            while (true)
            {
                var scanDump = _cuckooFilter.ScanDump(existingFilter, iterator);
                iterator = scanDump.Iterator;

                if (iterator == 0)
                {
                    break;
                }

                Assert.True(scanDump.Data.Length > 0);

                var loadResult = _cuckooFilter.LoadChunk(newFilter, iterator, scanDump.Data);
                Assert.True(loadResult);
            }
        }

        [Fact]
        public async Task ScanDumpAndLoadBasicAsync()
        {
            var existingFilter = FilterName();
            var newFilter = existingFilter + "New";

            await _cuckooFilter.InsertAsync(existingFilter, "foo", "bar", "baz");

            var iterator = 0L;
            while (true)
            {
                var scanDump = await _cuckooFilter.ScanDumpAsync(existingFilter, iterator);
                iterator = scanDump.Iterator;

                if (iterator == 0)
                {
                    break;
                }

                Assert.True(scanDump.Data.Length > 0);

                var loadResult = await _cuckooFilter.LoadChunkAsync(newFilter, iterator, scanDump.Data);
                Assert.True(loadResult);
            }
        }

        [Fact]
        public void InfoFilterExist()
        {
            var filterName = FilterName();

            const int capacity = 100;
            const int bucketSize = 10;
            const int expansion = 2;
            const int maxIterations = 5;

            _cuckooFilter.Reserve(filterName, capacity, bucketSize: bucketSize, expansion: expansion,
                maxIterations: maxIterations);

            var filterInfo = _cuckooFilter.Info(filterName);

            Assert.NotNull(filterInfo);
            Assert.True(filterInfo.Size > 0);
            Assert.Equal(expansion, filterInfo.ExpansionRate);
            Assert.Equal(1, filterInfo.NumberOfFilters);
            Assert.Equal(0, filterInfo.NumberOfItemsInserted);
            Assert.Equal(0, filterInfo.NumberOfItemsDeleted);
        }

        [Fact]
        public async Task InfoFilterExistAsync()
        {
            var filterName = FilterName();

            const int capacity = 100;
            const int bucketSize = 10;
            const int expansion = 2;
            const int maxIterations = 5;

            await _cuckooFilter.ReserveAsync(filterName, capacity, bucketSize: bucketSize, expansion: expansion,
                maxIterations: maxIterations);

            var filterInfo = await _cuckooFilter.InfoAsync(filterName);

            Assert.NotNull(filterInfo);
            Assert.True(filterInfo.Size > 0);
            Assert.Equal(expansion, filterInfo.ExpansionRate);
            Assert.Equal(1, filterInfo.NumberOfFilters);
            Assert.Equal(0, filterInfo.NumberOfItemsInserted);
            Assert.Equal(0, filterInfo.NumberOfItemsDeleted);
        }

        [Fact]
        public void InfoFilterNotExist()
        {
            Assert.Throws<RedisServerException>(() => _cuckooFilter.Info(FilterName()));
        }

        [Fact]
        public async Task InfoFilterNotExistAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() => _cuckooFilter.InfoAsync(FilterName()));
        }

        private string FilterName([CallerMemberName] string memberName = "")
        {
            return $"{nameof(CuckooFilterClientTests)}_{memberName}";
        }
    }
}
