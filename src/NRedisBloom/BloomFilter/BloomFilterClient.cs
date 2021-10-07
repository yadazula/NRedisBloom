using System.Collections.Generic;
using System.Threading.Tasks;
using NRedisBloom.Shared;
using StackExchange.Redis;

namespace NRedisBloom.BloomFilter
{
    /// <summary>
    /// Implements RedisBloom Bloom Filter commands
    /// </summary>
    public class BloomFilterClient
    {
        private readonly IDatabase _db;

        /// <summary>
        /// Creates a new client for Bloom Filter
        /// </summary>
        /// <param name="db">Database instance</param>
        public BloomFilterClient(IDatabase db) => _db = db;

        /// <summary>
        /// Creates an empty bloom filter with a single sub-filter for the initial capacity requested and with an upper bound error rate
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfreserve">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <param name="initCapacity">Number of entries intended to be added to the filter</param>
        /// <param name="errorRate">Desired probability for false positives</param>
        /// <param name="expansion">When capacity is reached, an additional sub-filter is created. The size of the new sub-filter is the size of the last sub-filter multiplied by expansion.</param>
        /// <param name="nonScaling">Prevents the filter from creating additional sub-filters if initial capacity is reached</param>
        /// <returns><code>true</code> if filter is created</returns>
        public bool Reserve(string name, long initCapacity, double errorRate,
            int? expansion = null, bool? nonScaling = null)
        {
            var args = BuildArgsForReserve(name, initCapacity, errorRate, expansion, nonScaling);

            var result = _db.Execute(Command.Reserve, args);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Creates an empty bloom filter with a single sub-filter for the initial capacity requested and with an upper bound error rate
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfreserve">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <param name="initCapacity">Number of entries intended to be added to the filter</param>
        /// <param name="errorRate">Desired probability for false positives</param>
        /// <param name="expansion">When capacity is reached, an additional sub-filter is created. The size of the new sub-filter is the size of the last sub-filter multiplied by expansion.</param>
        /// <param name="nonScaling">Prevents the filter from creating additional sub-filters if initial capacity is reached</param>
        /// <returns><code>true</code> if filter is created</returns>
        public async Task<bool> ReserveAsync(string name, long initCapacity, double errorRate,
            int? expansion = null, bool? nonScaling = null)
        {
            var args = BuildArgsForReserve(name, initCapacity, errorRate, expansion, nonScaling);

            var result = await _db.ExecuteAsync(Command.Reserve, args);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Adds an item to the filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfadd">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <param name="value">Value to add to the filter</param>
        /// <returns><code>true</code> if the item was not previously in the filter</returns>
        public bool Add(string name, string value)
        {
            var result = _db.Execute(Command.Add, name, value);

            return (bool)result;
        }

        /// <summary>
        /// Adds an item to the filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfadd">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <param name="value">Value to add to the filter</param>
        /// <returns>true if the item was not previously in the filter</returns>
        public async Task<bool> AddAsync(string name, string value)
        {
            var result = await _db.ExecuteAsync(Command.Add, name, value);

            return (bool)result;
        }

        /// <summary>
        /// Adds one or more items to a filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfmadd">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <param name="values">Values to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether the corresponding element was previously in the filter or not.
        /// A <code>true</code> value means the item did not previously exist,
        /// whereas a <code>false</code> value means it may have previously existed.
        /// </returns>
        public bool[] AddMultiple(string name, params string[] values)
        {
            var result = _db.Execute(Command.AddMultiple, name.PrependToArray(values));

            return (bool[])result;
        }

        /// <summary>
        /// Add one or more items to a filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfmadd">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <param name="values">Values to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether the corresponding element was previously in the filter or not.
        /// A <code>true</code> value means the item did not previously exist,
        /// whereas a <code>false</code> value means it may have previously existed.
        /// </returns>
        public async Task<bool[]> AddMultipleAsync(string name, params string[] values)
        {
            var result = await _db.ExecuteAsync(Command.AddMultiple, name.PrependToArray(values));

            return (bool[])result;
        }

        /// <summary>
        /// Adds one or more items to the bloom filter, by default creating it if it does not yet exist
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfinsert">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <param name="options"><see cref="InsertOptions"/></param>
        /// <param name="values">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether the corresponding element was previously in the filter or not.
        /// A <code>true</code> value means the item did not previously exist,
        /// whereas a <code>false</code> value means it may have previously existed.
        /// </returns>
        public bool[] Insert(string name, InsertOptions options,
            params string[] values)
        {
            var args = BuildArgsForInsert(name, options, values);

            var result = _db.Execute(Command.Insert, args);

            return (bool[])result;
        }

        /// <summary>
        /// Add one or more items to the bloom filter, by default creating it if it does not yet exist
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfinsert">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <param name="options"><see cref="InsertOptions"/></param>
        /// <param name="values">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether the corresponding element was previously in the filter or not.
        /// A <code>true</code> value means the item did not previously exist,
        /// whereas a <code>false</code> value means it may have previously existed.
        /// </returns>
        public async Task<bool[]> InsertAsync(string name, InsertOptions options,
            params string[] values)
        {
            var args = BuildArgsForInsert(name, options, values);

            var result = await _db.ExecuteAsync(Command.Insert, args);

            return (bool[])result;
        }

        /// <summary>
        /// Checks if an item exists in the filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfexists">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <param name="value">Value to check for</param>
        /// <returns><code>true</code> if the item may exist in the filter, <code>false</code> if the item does not exist in the filter</returns>
        public bool Exists(string name, string value)
        {
            var result = _db.Execute(Command.Exists, name, value);

            return (bool)result;
        }

        /// <summary>
        /// Check if an item exists in the filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfexists">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <param name="value">Value to check for</param>
        /// <returns>true if the item may exist in the filter, false if the item does not exist in the filter</returns>
        public async Task<bool> ExistsAsync(string name, string value)
        {
            var result = await _db.ExecuteAsync(Command.Exists, name, value);

            return (bool)result;
        }

        /// <summary>
        /// Checks if one or more items exist in the filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfmexists">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <param name="values">Values to check for</param>
        /// <returns>An array of booleans. A <code>true</code> value means the corresponding value may exist, <code>false</code> means it does not exist</returns>
        public bool[] ExistsMultiple(string name, params string[] values)
        {
            var result = _db.Execute(Command.ExistsMultiple, name.PrependToArray(values));

            return (bool[])result;
        }

        /// <summary>
        /// Check if one or more items exist in the filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfmexists">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <param name="values">Values to check for</param>
        /// <returns>An array of booleans. A <code>true</code> value means the corresponding value may exist, <code>false</code> means it does not exist</returns>
        public async Task<bool[]> ExistsMultipleAsync(string name, params string[] values)
        {
            var result = await _db.ExecuteAsync(Command.ExistsMultiple, name.PrependToArray(values));

            return (bool[])result;
        }

        /// <summary>
        /// Restores a filter previously saved using <see cref="ScanDump"/>.
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfloadchunk">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <param name="iterator">Iterator value associated with data (returned by <see cref="ScanDump"/>)</param>
        /// <param name="data">Current data chunk (returned by <see cref="ScanDump"/>)</param>
        /// <returns><code>true</code> if chunk is restored</returns>
        public bool LoadChunk(string name, long iterator, byte[] data)
        {
            var result = _db.Execute(Command.LoadChunk, name, iterator, data);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Restores a filter previously saved using <see cref="ScanDump"/>.
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfloadchunk">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <param name="iterator">Iterator value associated with data (returned by <see cref="ScanDump"/>)</param>
        /// <param name="data">Current data chunk (returned by <see cref="ScanDump"/>)</param>
        /// <returns><code>true</code> if chunk is restored</returns>
        public async Task<bool> LoadChunkAsync(string name, long iterator, byte[] data)
        {
            var result = await _db.ExecuteAsync(Command.LoadChunk, name, iterator, data);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Begins an incremental save of the bloom filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfscandump">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <param name="iterator">Iterator value; either 0 or the iterator from a previous invocation of this command</param>
        /// <returns>An pair of Iterator and Data. If Iterator is 0, then it means iteration has completed.</returns>
        public (long Iterator, byte[] Data) ScanDump(string name, long iterator)
        {
            var result = (RedisResult[])_db.Execute(Command.ScanDump, name, iterator);

            return ((long)result[0], (byte[])result[1]);
        }

        /// <summary>
        /// Begins an incremental save of the bloom filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfscandump">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <param name="iterator">Iterator value; either 0 or the iterator from a previous invocation of this command</param>
        /// <returns>An pair of Iterator and Data. If Iterator is 0, then it means iteration has completed.</returns>
        public async Task<(long Iterator, byte[] Data)> ScanDumpAsync(string name, long iterator)
        {
            var result = (RedisResult[])await _db.ExecuteAsync(Command.ScanDump, name, iterator);

            return ((long)result[0], (byte[])result[1]);
        }

        /// <summary>
        /// Gets information about the filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfinfo">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <returns>An instance of <see cref="InfoResult"/> that contains information about the filter</returns>
        public InfoResult Info(string name)
        {
            var result = _db.Execute(Command.Info, name);

            return InfoResult.Create((RedisResult[])result);
        }

        /// <summary>
        /// Get information about the filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfinfo">Command Reference</a>
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <returns>An instance of <see cref="InfoResult"/> that contains information about the filter</returns>
        public async Task<InfoResult> InfoAsync(string name)
        {
            var result = await _db.ExecuteAsync(Command.Info, name);

            return InfoResult.Create((RedisResult[])result);
        }

        private static ICollection<object> BuildArgsForReserve(string name, long initCapacity, double errorRate,
            int? expansion,
            bool? nonScaling)
        {
            var args = new List<object> { name, errorRate, initCapacity };

            if (expansion.HasValue)
            {
                args.Add(Keywords.Expansion);
                args.Add(expansion.Value);
            }

            if (nonScaling == true)
            {
                args.Add(Keywords.Nonscaling);
            }

            return args;
        }

        private static ICollection<object> BuildArgsForInsert(string name, InsertOptions options, string[] values)
        {
            var args = new List<object> { name };

            if (options?.Capacity.HasValue == true)
            {
                args.Add(Keywords.Capacity.ToUpperInvariant());
                args.Add(options.Capacity);
            }

            if (options?.ErrorRate.HasValue == true)
            {
                args.Add(Keywords.Error);
                args.Add(options.ErrorRate);
            }

            if (options?.Expansion.HasValue == true)
            {
                args.Add(Keywords.Expansion);
                args.Add(options.Expansion);
            }

            if (options?.NoCreate == true)
            {
                args.Add(Keywords.NoCreate);
            }

            if (options?.NonScaling == true)
            {
                args.Add(Keywords.Nonscaling);
            }

            args.Add(Keywords.Items);
            args.AddRange(values);

            return args;
        }
    }
}
