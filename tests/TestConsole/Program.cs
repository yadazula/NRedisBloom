using System;
using System.Text.Json;
using System.Threading.Tasks;
using NRedisBloom.BloomFilter;
using StackExchange.Redis;

namespace TestConsole
{
    class Program
    {
        static async Task Main(string[] args)
        {
            using (var muxr = ConnectionMultiplexer.Connect("localhost"))
            {
                var db = muxr.GetDatabase();

                var filterName = "newFilter";
                var filterName2 = "newFilter2";

                // var result = await db.BloomFilterReserveAsync(filterName, 1000, 0.01, nonScaling: false);
                // Console.Out.WriteLine("result = {0}", result);

                var insertOptions = new InsertOptions
                {
                    Capacity = 1000,
                    Expansion = 4,
                    ErrorRate = 0.01,
                    NoCreate = false,
                    NonScaling = false
                };
                var result = await db.BloomFilterInsertAsync(filterName, insertOptions, "foo", "bar");
                Console.Out.WriteLine("result = {0}", result[0]);
                Console.Out.WriteLine("result = {0}", result[1]);

                var iterator = 0L;
                while (true)
                {
                    var scanDump = await db.BloomFilterScanDumpAsync(filterName, iterator);
                    iterator = scanDump.Iterator;

                    if (iterator == 0)
                    {
                        break;
                    }

                    Console.Out.WriteLine("dump = {0}", scanDump);

                    var chunk = await db.BloomFilterLoadChunkAsync(filterName2, iterator, scanDump.Data);
                    Console.Out.WriteLine("chunk = {0}", chunk);
                }

                // var result = await db.BloomFilterAddAsync("newFilter", "foo");
                // Console.Out.WriteLine("result = {0}", result);

                // result = await db.BloomFilterExistsAsync("newFilter", "foo");
                // Console.Out.WriteLine("result = {0}", result);
                //
                // result = await db.BloomFilterExistsAsync("newFilter", "bar");
                // Console.Out.WriteLine("result = {0}", result);

                // var results = await db.BloomFilterAddMultipleAsync(filterName, "foo", "bar");
                // Console.Out.WriteLine("result[0] = {0}", results[0]);
                // Console.Out.WriteLine("result[1] = {0}", results[1]);
                //
                // results = await db.BloomFilterExistsMultipleAsync(filterName, "foo", "bar");
                // Console.Out.WriteLine("result[0] = {0}", results[0]);
                // Console.Out.WriteLine("result[1] = {0}", results[1]);
                //
                // results = await db.BloomFilterExistsMultipleAsync(filterName, "foo2", "bar2");
                // Console.Out.WriteLine("result[0] = {0}", results[0]);
                // Console.Out.WriteLine("result[1] = {0}", results[1]);

                var infoResult = await db.BloomFilterInfoAsync(filterName);

                Console.Out.WriteLine("infoResult = {0}", JsonSerializer.Serialize(infoResult));

                db.KeyDelete(filterName);

                db.KeyDelete(filterName2);
            }
        }
    }
}
