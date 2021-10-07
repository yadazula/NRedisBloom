using System;
using StackExchange.Redis;
using Xunit;

namespace NRedisBloom.Tests
{
    [Collection("Non-Parallel Collection")]
    public abstract class TestBase : IDisposable
    {
        private readonly ConnectionMultiplexer _muxer;
        protected IDatabase Db { get; }

        protected TestBase()
        {
            var options = new ConfigurationOptions
            {
                EndPoints = { "127.0.0.1:6379" },
                AllowAdmin = true
            };
            _muxer = ConnectionMultiplexer.Connect(options);

            Db = _muxer.GetDatabase();

            var server = _muxer.GetServer(_muxer.GetEndPoints()[0]);
            server.FlushDatabase();
        }

        public void Dispose()
        {
            _muxer.Dispose();
        }
    }

    [CollectionDefinition("Non-Parallel Collection", DisableParallelization = true)]
    [TestCaseOrderer("Xunit2AcceptanceTests+TestOrdering+AlphabeticalOrderer", "test.xunit.execution")]
    public class TestClassNonParallelCollectionDefinition { }
}
