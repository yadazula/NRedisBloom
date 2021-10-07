using System.Collections.Generic;
using System.Threading.Tasks;
using NRedisBloom.Shared;
using StackExchange.Redis;

namespace NRedisBloom.CuckooFilter
{
    /// <summary>
    /// Implements RedisBloom Cuckoo Filter commands
    /// </summary>
    public class CuckooFilterClient
    {
        private readonly IDatabase _db;

        /// <summary>
        /// Creates a new client for Cuckoo Filter
        /// </summary>
        public CuckooFilterClient(IDatabase db) => _db = db;

        /// <summary>
        /// Create a Cuckoo Filter as key with a single sub-filter for the initial amount of capacity for items
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfreserve">Command Reference</a>
        /// </summary>
        /// <param name="key">The key under which the filter is found</param>
        /// <param name="capacity">Estimated capacity for the filter</param>
        /// <param name="bucketSize">Number of items in each bucket</param>
        /// <param name="maxIterations">Number of attempts to swap items between buckets before declaring filter as full and creating an additional filter</param>
        /// <param name="expansion">When a new filter is created, its size is the size of the current filter multiplied by expansion</param>
        /// <returns><code>true</code> if filter is created</returns>
        public bool Reserve(string key, long capacity, long? bucketSize = null,
            long? maxIterations = null, long? expansion = null)
        {
            var args = BuildArgsForReserve(key, capacity, bucketSize, maxIterations, expansion);

            var result = _db.Execute(Command.Reserve, args);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Create a Cuckoo Filter as key with a single sub-filter for the initial amount of capacity for items
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfreserve">Command Reference</a>
        /// </summary>
        /// <param name="key">The key under which the filter is found</param>
        /// <param name="capacity">Estimated capacity for the filter</param>
        /// <param name="bucketSize">Number of items in each bucket</param>
        /// <param name="maxIterations">Number of attempts to swap items between buckets before declaring filter as full and creating an additional filter</param>
        /// <param name="expansion">When a new filter is created, its size is the size of the current filter multiplied by expansion</param>
        /// <returns><code>true</code> if filter is created</returns>
        public async Task<bool> ReserveAsync(string key, long capacity,
            long? bucketSize = null,
            long? maxIterations = null, long? expansion = null)
        {
            var args = BuildArgsForReserve(key, capacity, bucketSize, maxIterations, expansion);

            var result = await _db.ExecuteAsync(Command.Reserve, args);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Adds an item to the Cuckoo Filter, creating the filter if it does not exist
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfadd">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="item">Item to add</param>
        /// <returns><code>true</code> on success, otherwise <code>false</code></returns>
        public bool Add(string key, string item)
        {
            var result = _db.Execute(Command.Add, key, item);

            return (bool)result;
        }

        /// <summary>
        /// Adds an item to the Cuckoo Filter, creating the filter if it does not exist
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfadd">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="item">Item to add</param>
        /// <returns><code>true</code> on success, otherwise <code>false</code></returns>
        public async Task<bool> AddAsync(string key, string item)
        {
            var result = await _db.ExecuteAsync(Command.Add, key, item);

            return (bool)result;
        }

        /// <summary>
        /// Adds an item to a Cuckoo Filter if the item did not exist previously
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfaddnx">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="item">Item to add</param>
        /// <returns><code>true</code> item was added to the filter, <code>false</code> if the item already exists</returns>
        public bool AddAdvanced(string key, string item)
        {
            var result = _db.Execute(Command.AddAdvanced, key, item);

            return (bool)result;
        }

        /// <summary>
        /// Adds an item to a Cuckoo Filter if the item did not exist previously
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfaddnx">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="item">Item to add</param>
        /// <returns><code>true</code> item was added to the filter, <code>false</code> if the item already exists</returns>
        public async Task<bool> AddAdvancedAsync(string key, string item)
        {
            var result = await _db.ExecuteAsync(Command.AddAdvanced, key, item);

            return (bool)result;
        }

        /// <summary>
        /// Adds one or more items to a Cuckoo Filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinsert">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="items">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether corresponding item is inserted.
        /// </returns>
        public bool[] Insert(string key, params string[] items)
        {
            var args = BuildArgsForInsert(key, null, items);

            var result = _db.Execute(Command.Insert, args);

            return (bool[])result;
        }

        /// <summary>
        /// Adds one or more items to a Cuckoo Filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinsert">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="options"><see cref="InsertOptions"/></param>
        /// <param name="items">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether corresponding item is inserted.
        /// </returns>
        public bool[] Insert(string key, InsertOptions options,
            params string[] items)
        {
            var args = BuildArgsForInsert(key, options, items);

            var result = _db.Execute(Command.Insert, args);

            return (bool[])result;
        }

        /// <summary>
        /// Adds one or more items to a Cuckoo Filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinsert">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="items">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether corresponding item is inserted.
        /// </returns>
        public async Task<bool[]> InsertAsync(string key, params string[] items)
        {
            var args = BuildArgsForInsert(key, null, items);

            var result = await _db.ExecuteAsync(Command.Insert, args);

            return (bool[])result;
        }

        /// <summary>
        /// Adds one or more items to a Cuckoo Filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinsert">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="options"><see cref="InsertOptions"/></param>
        /// <param name="items">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether corresponding item is inserted.
        /// </returns>
        public async Task<bool[]> InsertAsync(string key, InsertOptions options,
            params string[] items)
        {
            var args = BuildArgsForInsert(key, options, items);

            var result = await _db.ExecuteAsync(Command.Insert, args);

            return (bool[])result;
        }

        /// <summary>
        /// Adds one or more items to a Cuckoo Filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinsertnx">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="items">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether corresponding item is inserted.
        /// </returns>
        public bool[] InsertAdvanced(string key, params string[] items)
        {
            var args = BuildArgsForInsert(key, null, items);

            var result = _db.Execute(Command.InsertAdvanced, args);

            return (bool[])result;
        }

        /// <summary>
        /// Adds one or more items to a Cuckoo Filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinsertnx">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="options"><see cref="InsertOptions"/></param>
        /// <param name="items">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether corresponding item is inserted.
        /// </returns>
        public bool[] InsertAdvanced(string key, InsertOptions options,
            params string[] items)
        {
            var args = BuildArgsForInsert(key, options, items);

            var result = _db.Execute(Command.InsertAdvanced, args);

            return (bool[])result;
        }

        /// <summary>
        /// Adds one or more items to a Cuckoo Filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinsertnx">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="items">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether corresponding item is inserted.
        /// </returns>
        public async Task<bool[]> InsertAdvancedAsync(string key, params string[] items)
        {
            var args = BuildArgsForInsert(key, null, items);

            var result = await _db.ExecuteAsync(Command.InsertAdvanced, args);

            return (bool[])result;
        }

        /// <summary>
        /// Adds one or more items to a Cuckoo Filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinsertnx">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="options"><see cref="InsertOptions"/></param>
        /// <param name="items">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether corresponding item is inserted.
        /// </returns>
        public async Task<bool[]> InsertAdvancedAsync(string key, InsertOptions options,
            params string[] items)
        {
            var args = BuildArgsForInsert(key, options, items);

            var result = await _db.ExecuteAsync(Command.InsertAdvanced, args);

            return (bool[])result;
        }

        /// <summary>
        /// Check if an item exists in a Cuckoo Filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfexists">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="item">Item to check for</param>
        /// <returns><code>true</code> if the item may exist in the filter, <code>false</code> if the item does not exist in the filter</returns>
        public bool Exists(string key, string item)
        {
            var result = _db.Execute(Command.Exists, key, item);

            return (bool)result;
        }

        /// <summary>
        /// Check if an item exists in a Cuckoo Filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfexists">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="item">Item to check for</param>
        /// <returns><code>true</code> if the item may exist in the filter, <code>false</code> if the item does not exist in the filter</returns>
        public async Task<bool> ExistsAsync(string key, string item)
        {
            var result = await _db.ExecuteAsync(Command.Exists, key, item);

            return (bool)result;
        }

        /// <summary>
        /// Deletes an item once from the filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfdel">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="item">Item to delete from the filter</param>
        /// <returns><code>true</code> if the item has been deleted, <code>false</code> if the item was not found</returns>
        public bool Delete(string key, string item)
        {
            var result = _db.Execute(Command.Delete, key, item);

            return (bool)result;
        }

        /// <summary>
        /// Deletes an item once from the filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfdel">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="item"></param>
        /// <returns><code>true</code> if the item has been deleted, <code>false</code> if the item was not found</returns>
        public async Task<bool> DeleteAsync(string key, string item)
        {
            var result = await _db.ExecuteAsync(Command.Delete, key, item);

            return (bool)result;
        }

        /// <summary>
        /// Returns the number of times an item may be in the filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfcount">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="item">Item to count</param>
        /// <returns>The number of times the item exists in the filter</returns>
        public long Count(string key, string item)
        {
            var result = _db.Execute(Command.Count, key, item);

            return (long)result;
        }

        /// <summary>
        /// Returns the number of times an item may be in the filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfcount">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="item">Item to count</param>
        /// <returns>The number of times the item exists in the filter</returns>
        public async Task<long> CountAsync(string key, string item)
        {
            var result = await _db.ExecuteAsync(Command.Count, key, item);

            return (long)result;
        }

        /// <summary>
        /// Begins an incremental save of the Cuckoo Filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfscandump">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="iterator">Iterator value; either 0 or the iterator from a previous invocation of this command</param>
        /// <returns>An pair of Iterator and Data. If Iterator is 0, then it means iteration has completed.</returns>
        public (long Iterator, byte[] Data) ScanDump(string key, long iterator)
        {
            var result = (RedisResult[])_db.Execute(Command.ScanDump, key, iterator);

            return ((long)result[0], (byte[])result[1]);
        }

        /// <summary>
        /// Begins an incremental save of the Cuckoo Filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfscandump">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="iterator">Iterator value; either 0 or the iterator from a previous invocation of this command</param>
        /// <returns>An pair of Iterator and Data. If Iterator is 0, then it means iteration has completed.</returns>
        public async Task<(long Iterator, byte[] Data)> ScanDumpAsync(string key, long iterator)
        {
            var result = (RedisResult[])await _db.ExecuteAsync(Command.ScanDump, key, iterator);

            return ((long)result[0], (byte[])result[1]);
        }

        /// <summary>
        /// Restores a filter previously saved using <see cref="ScanDump"/>.
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfloadchunk">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="iterator">Iterator value associated with data (returned by <see cref="ScanDump"/>)</param>
        /// <param name="data">Current data chunk (returned by <see cref="ScanDump"/>)</param>
        /// <returns><code>true</code> if chunk is restored</returns>
        public bool LoadChunk(string key, long iterator, byte[] data)
        {
            var result = _db.Execute(Command.LoadChunk, key, iterator, data);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Restores a filter previously saved using <see cref="ScanDump"/>.
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfloadchunk">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <param name="iterator">Iterator value associated with data (returned by <see cref="ScanDump"/>)</param>
        /// <param name="data">Current data chunk (returned by <see cref="ScanDump"/>)</param>
        /// <returns><code>true</code> if chunk is restored</returns>
        public async Task<bool> LoadChunkAsync(string key, long iterator, byte[] data)
        {
            var result = await _db.ExecuteAsync(Command.LoadChunk, key, iterator, data);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Gets information about the filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinfo">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <returns>An instance of <see cref="InfoResult"/> that contains information about the filter</returns>
        public InfoResult Info(string key)
        {
            var result = _db.Execute(Command.Info, key);

            return InfoResult.Create((RedisResult[])result);
        }

        /// <summary>
        /// Gets information about the filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinfo">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the filter</param>
        /// <returns>An instance of <see cref="InfoResult"/> that contains information about the filter</returns>
        public async Task<InfoResult> InfoAsync(string key)
        {
            var result = await _db.ExecuteAsync(Command.Info, key);

            return InfoResult.Create((RedisResult[])result);
        }

        private static List<object> BuildArgsForReserve(string key, long capacity, long? bucketSize,
            long? maxIterations,
            long? expansion)
        {
            var args = new List<object> { key, capacity };

            if (bucketSize.HasValue)
            {
                args.Add(Keywords.BucketSize);
                args.Add(bucketSize.Value);
            }

            if (maxIterations.HasValue)
            {
                args.Add(Keywords.MaxIterations);
                args.Add(maxIterations.Value);
            }

            if (expansion.HasValue)
            {
                args.Add(Keywords.Expansion);
                args.Add(expansion.Value);
            }

            return args;
        }

        private static List<object> BuildArgsForInsert(string key, InsertOptions options, string[] items)
        {
            var args = new List<object> { key };

            if (options?.Capacity.HasValue == true)
            {
                args.Add(Keywords.Capacity.ToUpperInvariant());
                args.Add(options.Capacity);
            }

            if (options?.NoCreate == true)
            {
                args.Add(Keywords.NoCreate);
            }

            args.Add(Keywords.Items);
            args.AddRange(items);
            return args;
        }
    }
}
