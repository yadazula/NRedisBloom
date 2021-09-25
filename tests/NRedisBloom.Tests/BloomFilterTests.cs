using System.Threading.Tasks;
using NRedisBloom.BloomFilter;
using StackExchange.Redis;
using Xunit;

namespace NRedisBloom.Tests
{
    public class BloomFilterTests : TestBase
    {
        [Fact]
        public void ReserveBasic()
        {
            const string filterName = nameof(ReserveBasic);

            Assert.True(Db.BloomFilterReserve(filterName, 100, 0.001));
            Assert.True(Db.BloomFilterAdd(filterName, "val1"));
            Assert.True(Db.BloomFilterExists(filterName, "val1"));
            Assert.False(Db.BloomFilterExists(filterName, "val2"));
        }

        [Fact]
        public async Task ReserveBasicAsync()
        {
            const string filterName = nameof(ReserveBasic);

            Assert.True(await Db.BloomFilterReserveAsync(filterName, 100, 0.001));
            Assert.True(await Db.BloomFilterAddAsync(filterName, "val1"));
            Assert.True(await Db.BloomFilterExistsAsync(filterName, "val1"));
            Assert.False(await Db.BloomFilterExistsAsync(filterName, "val2"));
        }

        [Fact]
        public void ReserveValidateZeroCapacity()
        {
            Assert.Throws<RedisServerException>(() =>
                Db.BloomFilterReserve(nameof(ReserveValidateZeroCapacity), 0, 0.001));
        }

        [Fact]
        public async Task ReserveValidateZeroCapacityAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() =>
                Db.BloomFilterReserveAsync(nameof(ReserveValidateZeroCapacityAsync), 0, 0.001));
        }

        [Fact]
        public void ReserveValidateZeroError()
        {
            Assert.Throws<RedisServerException>(() =>
                Db.BloomFilterReserve(nameof(ReserveValidateZeroError), 100, 0));
        }

        [Fact]
        public async Task ReserveValidateZeroErrorAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() =>
                Db.BloomFilterReserveAsync(nameof(ReserveValidateZeroErrorAsync), 100, 0));
        }

        [Fact]
        public void ReserveAlreadyExists()
        {
            const string filterName = nameof(ReserveAlreadyExists);

            Assert.True(Db.BloomFilterReserve(filterName, 100, 0.1));

            Assert.Throws<RedisServerException>(() => Db.BloomFilterReserve(filterName, 100, 0.1));
        }

        [Fact]
        public async Task ReserveAlreadyExistsAsync()
        {
            const string filterName = nameof(ReserveAlreadyExistsAsync);

            Assert.True(await Db.BloomFilterReserveAsync(filterName, 100, 0.1));

            await Assert.ThrowsAsync<RedisServerException>(() => Db.BloomFilterReserveAsync(filterName, 100, 0.1));
        }

        [Fact]
        public void ReserveNonScalingExpand()
        {
            Assert.Throws<RedisServerException>(() =>
                Db.BloomFilterReserve(nameof(ReserveValidateZeroCapacity), 10, 0.01, expansion: 2, nonScaling: true));
        }

        [Fact]
        public async Task ReserveNonScalingExpandAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() =>
                Db.BloomFilterReserveAsync(nameof(ReserveValidateZeroCapacity), 10, 0.01, expansion: 2, nonScaling: true));
        }

        [Fact]
        public void InsertBasic()
        {
            const string filterName = nameof(InsertBasic);

            var insertOptions = new InsertOptions
            {
                Capacity = 1000,
                Expansion = 4,
                ErrorRate = 0.01,
                NoCreate = false,
                NonScaling = true
            };

            var result = Db.BloomFilterInsert(filterName, insertOptions, "foo", "bar");

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
                Db.BloomFilterInsert(nameof(InsertNoCreateFilterNotExist), insertOptions, "foo"));
        }

        [Fact]
        public async Task InsertBasicAsync()
        {
            const string filterName = nameof(InsertBasicAsync);

            var insertOptions = new InsertOptions
            {
                Capacity = 1000,
                Expansion = 4,
                ErrorRate = 0.01,
                NoCreate = false,
                NonScaling = true
            };

            var result = await Db.BloomFilterInsertAsync(filterName, insertOptions, "foo", "bar");

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
                Db.BloomFilterInsertAsync(nameof(InsertNoCreateFilterNotExistAsync), insertOptions, "foo"));
        }

        [Fact]
        public void AddExistString()
        {
            const string filterName = nameof(AddExistString);

            Assert.True(Db.BloomFilterAdd(filterName, "foo"));
            Assert.True(Db.BloomFilterExists(filterName, "foo"));
            Assert.False(Db.BloomFilterExists(filterName, "bar"));
            Assert.False(Db.BloomFilterAdd(filterName, "foo"));
        }

        [Fact]
        public async Task AddExistStringAsync()
        {
            const string filterName = nameof(AddExistString);

            Assert.True(await Db.BloomFilterAddAsync(filterName, "foo"));
            Assert.True(await Db.BloomFilterExistsAsync(filterName, "foo"));
            Assert.False(await Db.BloomFilterExistsAsync(filterName, "bar"));
            Assert.False(await Db.BloomFilterAddAsync(filterName, "foo"));
        }

        [Fact]
        public void TestExistsNonExist()
        {
            Assert.False(Db.BloomFilterExists(nameof(TestExistsNonExist), "foo"));
        }

        [Fact]
        public async Task TestExistsNonExistAsync()
        {
            Assert.False(await Db.BloomFilterExistsAsync(nameof(TestExistsNonExistAsync), "foo"));
        }

        [Fact]
        public void AddExistsMulti()
        {
            const string filterName = nameof(AddExistString);

            var result = Db.BloomFilterAddMultiple(filterName, "foo", "bar", "baz");
            Assert.Equal(3, result.Length);
            foreach (var item in result)
            {
                Assert.True(item);
            }

            result = Db.BloomFilterExistsMultiple(filterName, "foo", "bar", "baz");
            Assert.Equal(3, result.Length);
            foreach (var item in result)
            {
                Assert.True(item);
            }

            result = Db.BloomFilterAddMultiple(filterName, "newElem", "bar", "baz");
            Assert.Equal(3, result.Length);
            Assert.True(result[0]);
            Assert.False(result[1]);
            Assert.False(result[2]);

            result = Db.BloomFilterExistsMultiple(filterName, "foo", "notExist", "bar");
            Assert.Equal(3, result.Length);
            Assert.True(result[0]);
            Assert.False(result[1]);
            Assert.True(result[2]);
        }

        [Fact]
        public async Task AddExistsMultiAsync()
        {
            const string filterName = nameof(AddExistStringAsync);

            var result = await Db.BloomFilterAddMultipleAsync(filterName, "foo", "bar", "baz");
            Assert.Equal(3, result.Length);
            foreach (var item in result)
            {
                Assert.True(item);
            }

            result = await Db.BloomFilterExistsMultipleAsync(filterName, "foo", "bar", "baz");
            Assert.Equal(3, result.Length);
            foreach (var item in result)
            {
                Assert.True(item);
            }

            result = await Db.BloomFilterAddMultipleAsync(filterName, "newElem", "bar", "baz");
            Assert.Equal(3, result.Length);
            Assert.True(result[0]);
            Assert.False(result[1]);
            Assert.False(result[2]);

            result = await Db.BloomFilterExistsMultipleAsync(filterName, "foo", "notExist", "bar");
            Assert.Equal(3, result.Length);
            Assert.True(result[0]);
            Assert.False(result[1]);
            Assert.True(result[2]);
        }

        [Fact]
        public void InfoFilterExist()
        {
            const string filterName = nameof(InfoFilterExist);
            const int capacity = 100;
            const int expansion = 2;

            Db.BloomFilterReserve(filterName, capacity, 0.01, expansion: expansion);

            var filterInfo = Db.BloomFilterInfo(filterName);
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
            const string filterName = nameof(InfoFilterExistAsync);
            const int capacity = 100;
            const int expansion = 2;

            await Db.BloomFilterReserveAsync(filterName, capacity, 0.01, expansion: expansion);

            var filterInfo = await Db.BloomFilterInfoAsync(filterName);
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
            Assert.Throws<RedisServerException>(() => Db.BloomFilterInfo(nameof(InfoFilterNotExist)));
        }

        [Fact]
        public async Task InfoFilterNotExistAsync()
        {
            await Assert.ThrowsAsync<RedisServerException>(() => Db.BloomFilterInfoAsync(nameof(InfoFilterNotExist)));
        }

        [Fact]
        public void ScanDumpAndLoadBasic()
        {
            const string existingFilter = nameof(ScanDumpAndLoadBasic);
            const string newFilter = nameof(ScanDumpAndLoadBasic) + "New";

            Db.BloomFilterAddMultiple(existingFilter, "foo", "bar", "baz");

            var iterator = 0L;
            while (true)
            {
                var scanDump = Db.BloomFilterScanDump(existingFilter, iterator);
                iterator = scanDump.Iterator;

                if (iterator == 0)
                {
                    break;
                }

                Assert.True(scanDump.Data.Length > 0);

                var loadResult = Db.BloomFilterLoadChunk(newFilter, iterator, scanDump.Data);
                Assert.True(loadResult);
            }
        }

        [Fact]
        public async Task ScanDumpAndLoadBasicAsync()
        {
            const string existingFilter = nameof(ScanDumpAndLoadBasic);
            const string newFilter = nameof(ScanDumpAndLoadBasic) + "New";

            await Db.BloomFilterAddMultipleAsync(existingFilter, "foo", "bar", "baz");

            var iterator = 0L;
            while (true)
            {
                var scanDump = await Db.BloomFilterScanDumpAsync(existingFilter, iterator);
                iterator = scanDump.Iterator;

                if (iterator == 0)
                {
                    break;
                }

                Assert.True(scanDump.Data.Length > 0);

                var loadResult = await Db.BloomFilterLoadChunkAsync(newFilter, iterator, scanDump.Data);
                Assert.True(loadResult);
            }
        }
    }
}
