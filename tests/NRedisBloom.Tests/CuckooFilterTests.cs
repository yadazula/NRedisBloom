using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using NRedisBloom.CuckooFilter;
using StackExchange.Redis;
using Xunit;

namespace NRedisBloom.Tests
{
    public class CuckooFilterTests : TestBase
    {
        [Fact]
        public void ReserveBasic()
        {
            Assert.True(Db.CuckooFilterReserve(FilterName(), 100));
        }

        [Fact]
        public async Task ReserveBasicAsync()
        {
            Assert.True(await Db.CuckooFilterReserveAsync(FilterName(), 100));
        }

        [Fact]
        public void ReserveWithOptionalParameters()
        {
            Assert.True(Db.CuckooFilterReserve(FilterName(), 100, 8, 20, 2));
        }

        [Fact]
        public async Task ReserveWithOptionalParametersAsync()
        {
            Assert.True(await Db.CuckooFilterReserveAsync(FilterName(), 100, 8, 20, 2));
        }

        [Fact]
        public void AddBasic()
        {
            Assert.True(Db.CuckooFilterAdd(FilterName(), "foo"));
        }

        [Fact]
        public async Task AddBasicAsync()
        {
            Assert.True(await Db.CuckooFilterAddAsync(FilterName(), "foo"));
        }

        [Fact]
        public void AddAdvanced()
        {
            Assert.True(Db.CuckooFilterAddAdvanced(FilterName(), "foo"));
        }

        [Fact]
        public async Task AddAdvancedAsync()
        {
            Assert.True(await Db.CuckooFilterAddAdvancedAsync(FilterName(), "foo"));
        }

        [Fact]
        public void AddAdvancedExists()
        {
            var filterName = FilterName();

            Db.CuckooFilterAdd(filterName, "foo");

            Assert.False(Db.CuckooFilterAddAdvanced(filterName, "foo"));
        }

        [Fact]
        public async Task AddAdvancedExistsAsync()
        {
            var filterName = FilterName();

            await Db.CuckooFilterAddAsync(filterName, "foo");

            Assert.False(await Db.CuckooFilterAddAdvancedAsync(filterName, "foo"));
        }

        [Fact]
        public void InsertBasic()
        {
            var insertOptions = new InsertOptions { Capacity = 1000, NoCreate = false, };

            var result = Db.CuckooFilterInsert(FilterName(), insertOptions, "foo", "bar");

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

            var result = await Db.CuckooFilterInsertAsync(FilterName(), insertOptions, "foo", "bar");

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
                Db.CuckooFilterInsert(FilterName(), insertOptions, "foo"));
        }

        [Fact]
        public async Task InsertNoCreateFilterNotExistAsync()
        {
            var insertOptions = new InsertOptions { NoCreate = true };

            await Assert.ThrowsAsync<RedisServerException>(() =>
                Db.CuckooFilterInsertAsync(FilterName(), insertOptions, "foo"));
        }

        [Fact]
        public void InsertAdvancedBasic()
        {
            var insertOptions = new InsertOptions { Capacity = 1000, NoCreate = false, };

            var result = Db.CuckooFilterInsertAdvanced(FilterName(), insertOptions, "foo", "bar");

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

            Db.CuckooFilterInsert(FilterName(), insertOptions, "foo", "bar");

            var result = Db.CuckooFilterInsertAdvanced(FilterName(), insertOptions, "foo", "bar");

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

            var result = await Db.CuckooFilterInsertAdvancedAsync(FilterName(), insertOptions, "foo", "bar");

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

            await Db.CuckooFilterInsertAsync(FilterName(), insertOptions, "foo", "bar");

            var result = await Db.CuckooFilterInsertAdvancedAsync(FilterName(), insertOptions, "foo", "bar");

            Assert.Equal(2, result.Length);
            foreach (var item in result)
            {
                Assert.False(item);
            }
        }

        [Fact]
        public void ExistsNonExist()
        {
            Assert.False(Db.CuckooFilterExists(FilterName(), "foo"));
        }

        [Fact]
        public async Task ExistsNonExistAsync()
        {
            Assert.False(await Db.CuckooFilterExistsAsync(FilterName(), "foo"));
        }

        [Fact]
        public void AddExist()
        {
            var filterName = FilterName();

            Assert.True(Db.CuckooFilterAdd(filterName, "foo"));
            Assert.True(Db.CuckooFilterExists(filterName, "foo"));
            Assert.False(Db.CuckooFilterExists(filterName, "bar"));
        }

        [Fact]
        public async Task AddExistAsync()
        {
            var filterName = FilterName();

            Assert.True(await Db.CuckooFilterAddAsync(filterName, "foo"));
            Assert.True(await Db.CuckooFilterExistsAsync(filterName, "foo"));
            Assert.False(await Db.CuckooFilterExistsAsync(filterName, "bar"));
        }

        [Fact]
        public void DeleteBasic()
        {
            var filterName = FilterName();

            Db.CuckooFilterAdd(FilterName(), "foo");
            Assert.True(Db.CuckooFilterDelete(FilterName(), "foo"));
        }

        [Fact]
        public async Task DeleteBasicAsync()
        {
            var filterName = FilterName();

            await Db.CuckooFilterAddAsync(FilterName(), "foo");
            Assert.True(await Db.CuckooFilterDeleteAsync(FilterName(), "foo"));
        }

        [Fact]
        public void DeleteFilterNotExist()
        {
            Assert.Throws<RedisServerException>(() => Db.CuckooFilterDelete(FilterName(), "foo"));
        }

        [Fact]
        public void DeleteItemNotExist()
        {
            var filterName = FilterName();

            Db.CuckooFilterReserve(filterName, 100);

            Assert.False(Db.CuckooFilterDelete(filterName, "bar"));
        }

        [Fact]
        public async Task DeleteFilterNotExistAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() => Db.CuckooFilterDeleteAsync(FilterName(), "foo"));
        }

        [Fact]
        public async Task DeleteItemNotExistAsync()
        {
            var filterName = FilterName();

            await Db.CuckooFilterReserveAsync(filterName, 100);

            Assert.False(await Db.CuckooFilterDeleteAsync(filterName, "bar"));
        }

        [Fact]
        public void CountSingle()
        {
            var filterName = FilterName();

            Db.CuckooFilterAdd(filterName, "foo");

            Assert.Equal(1, Db.CuckooFilterCount(filterName, "foo"));
        }

        [Fact]
        public void CountMultiple()
        {
            var filterName = FilterName();

            Db.CuckooFilterAdd(filterName, "foo");
            Db.CuckooFilterAdd(filterName, "foo");

            Assert.Equal(2, Db.CuckooFilterCount(filterName, "foo"));
        }

        [Fact]
        public async Task CountSingleAsync()
        {
            var filterName = FilterName();

            await Db.CuckooFilterAddAsync(filterName, "foo");

            Assert.Equal(1, await Db.CuckooFilterCountAsync(filterName, "foo"));
        }

        [Fact]
        public async Task CountMultipleAsync()
        {
            var filterName = FilterName();

            await Db.CuckooFilterAddAsync(filterName, "foo");
            await Db.CuckooFilterAddAsync(filterName, "foo");

            Assert.Equal(2, await Db.CuckooFilterCountAsync(filterName, "foo"));
        }

        [Fact]
        public void ScanDumpAndLoadBasic()
        {
            var existingFilter = FilterName();
            var newFilter = existingFilter + "New";

            Db.CuckooFilterInsert(existingFilter, "foo", "bar");

            var iterator = 0L;
            while (true)
            {
                var scanDump = Db.CuckooFilterScanDump(existingFilter, iterator);
                iterator = scanDump.Iterator;

                if (iterator == 0)
                {
                    break;
                }

                Assert.True(scanDump.Data.Length > 0);

                var loadResult = Db.CuckooFilterLoadChunk(newFilter, iterator, scanDump.Data);
                Assert.True(loadResult);
            }
        }

        [Fact]
        public async Task ScanDumpAndLoadBasicAsync()
        {
            var existingFilter = FilterName();
            var newFilter = existingFilter + "New";

            await Db.CuckooFilterInsertAsync(existingFilter, "foo", "bar", "baz");

            var iterator = 0L;
            while (true)
            {
                var scanDump = await Db.CuckooFilterScanDumpAsync(existingFilter, iterator);
                iterator = scanDump.Iterator;

                if (iterator == 0)
                {
                    break;
                }

                Assert.True(scanDump.Data.Length > 0);

                var loadResult = await Db.CuckooFilterLoadChunkAsync(newFilter, iterator, scanDump.Data);
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

            Db.CuckooFilterReserve(filterName, capacity, bucketSize: bucketSize, expansion: expansion,
                maxIterations: maxIterations);

            var filterInfo = Db.CuckooFilterInfo(filterName);

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

            await Db.CuckooFilterReserveAsync(filterName, capacity, bucketSize: bucketSize, expansion: expansion,
                maxIterations: maxIterations);

            var filterInfo = await Db.CuckooFilterInfoAsync(filterName);

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
            Assert.Throws<RedisServerException>(() => Db.CuckooFilterInfo(FilterName()));
        }

        [Fact]
        public async Task InfoFilterNotExistAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() => Db.CuckooFilterInfoAsync(FilterName()));
        }

        private string FilterName([CallerMemberName] string memberName = "")
        {
            return $"{nameof(CuckooFilterTests)}_{memberName}";
        }
    }
}
