using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using NRedisBloom.CuckooFilter;
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

        private string FilterName([CallerMemberName] string memberName = "")
        {
            return $"{nameof(CuckooFilterTests)}_{memberName}";
        }
    }
}
