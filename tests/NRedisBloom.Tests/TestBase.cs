using System;
using StackExchange.Redis;

namespace NRedisBloom.Tests
{
    public abstract class TestBase : IDisposable
    {
        private ConnectionMultiplexer _muxer;
        protected IDatabase Db { get; private set; }

        protected TestBase()
        {
            var options = new ConfigurationOptions
            {
                EndPoints = { "127.0.0.1:6379" },
                AllowAdmin = true,
                ConnectTimeout = 2000,
                SyncTimeout = 15000,
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
}
