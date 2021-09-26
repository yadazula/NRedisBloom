using System.Collections.Generic;
using NRedisBloom.Shared;
using StackExchange.Redis;

namespace NRedisBloom.BloomFilter
{
    /// <summary>
    /// This class defines the extension methods for <see cref="StackExchange.Redis.IDatabase"/>
    /// that allow for the interaction with the RedisBloom (2.x) module.
    /// </summary>
    public static partial class DatabaseExtensions
    {
        /// <summary>
        /// Creates an empty bloom filter with a single sub-filter for the initial capacity requested and with an upper bound error rate
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfreserve">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="name">Name of the filter</param>
        /// <param name="initCapacity">Number of entries intended to be added to the filter</param>
        /// <param name="errorRate">Desired probability for false positives</param>
        /// <param name="expansion">When capacity is reached, an additional sub-filter is created. The size of the new sub-filter is the size of the last sub-filter multiplied by expansion.</param>
        /// <param name="nonScaling">Prevents the filter from creating additional sub-filters if initial capacity is reached</param>
        /// <returns><code>true</code> if filter is created</returns>
        public static bool BloomFilterReserve(this IDatabase db, string name, long initCapacity, double errorRate,
            int? expansion = null, bool? nonScaling = null)
        {
            var args = BuildArgsForReserve(name, initCapacity, errorRate, expansion, nonScaling);

            var result = db.Execute(Command.Reserve, args);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Adds an item to the filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfadd">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="name">Name of the filter</param>
        /// <param name="value">Value to add to the filter</param>
        /// <returns><code>true</code> if the item was not previously in the filter</returns>
        public static bool BloomFilterAdd(this IDatabase db, string name, string value)
        {
            var result = db.Execute(Command.Add, name, value);

            return (bool) result;
        }

        /// <summary>
        /// Adds one or more items to a filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfmadd">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="name">Name of the filter</param>
        /// <param name="values">Values to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether the corresponding element was previously in the filter or not.
        /// A <code>true</code> value means the item did not previously exist,
        /// whereas a <code>false</code> value means it may have previously existed.
        /// </returns>
        public static bool[] BloomFilterAddMultiple(this IDatabase db, string name, params string[] values)
        {
            var result = db.Execute(Command.AddMultiple, name.PrependToArray(values));

            return (bool[]) result;
        }

        /// <summary>
        /// Adds one or more items to the bloom filter, by default creating it if it does not yet exist
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfinsert">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="name">Name of the filter</param>
        /// <param name="options"><see cref="InsertOptions"/></param>
        /// <param name="values">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether the corresponding element was previously in the filter or not.
        /// A <code>true</code> value means the item did not previously exist,
        /// whereas a <code>false</code> value means it may have previously existed.
        /// </returns>
        public static bool[] BloomFilterInsert(this IDatabase db, string name, InsertOptions options,
            params string[] values)
        {
            var args = BuildArgsForInsert(name, options, values);

            var result = db.Execute(Command.Insert, args);

            return (bool[]) result;
        }

        /// <summary>
        /// Checks if an item exists in the filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfexists">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="name">Name of the filter</param>
        /// <param name="value">Value to check for</param>
        /// <returns><code>true</code> if the item may exist in the filter, <code>false</code> if the item does not exist in the filter</returns>
        public static bool BloomFilterExists(this IDatabase db, string name, string value)
        {
            var result = db.Execute(Command.Exists, name, value);

            return (bool) result;
        }

        /// <summary>
        /// Checks if one or more items exist in the filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfmexists">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="name">Name of the filter</param>
        /// <param name="values">Values to check for</param>
        /// <returns>An array of booleans. A <code>true</code> value means the corresponding value may exist, <code>false</code> means it does not exist</returns>
        public static bool[] BloomFilterExistsMultiple(this IDatabase db, string name, params string[] values)
        {
            var result = db.Execute(Command.ExistsMultiple, name.PrependToArray(values));

            return (bool[]) result;
        }

        /// <summary>
        /// Gets information about the filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfinfo">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="name">Name of the filter</param>
        /// <returns>An instance of <see cref="InfoResult"/> that contains information about the filter</returns>
        public static InfoResult BloomFilterInfo(this IDatabase db, string name)
        {
            var result = db.Execute(Command.Info, name);

            return InfoResult.Create((RedisResult[]) result);
        }

        /// <summary>
        /// Begins an incremental save of the bloom filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfscandump">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="name">Name of the filter</param>
        /// <param name="iterator">Iterator value; either 0 or the iterator from a previous invocation of this command</param>
        /// <returns>An pair of Iterator and Data. If Iterator is 0, then it means iteration has completed.</returns>
        public static (long Iterator, byte[] Data) BloomFilterScanDump(this IDatabase db, string name, long iterator)
        {
            var result = (RedisResult[]) db.Execute(Command.ScanDump, name, iterator);

            return ((long) result[0], (byte[]) result[1]);
        }

        /// <summary>
        /// Restores a filter previously saved using <see cref="BloomFilterScanDump"/>.
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfloadchunk">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="name">Name of the filter</param>
        /// <param name="iterator">Iterator value associated with data (returned by <see cref="BloomFilterScanDump"/>)</param>
        /// <param name="data">Current data chunk (returned by <see cref="BloomFilterScanDump"/>)</param>
        /// <returns><code>true</code> if chunk is restored</returns>
        public static bool BloomFilterLoadChunk(this IDatabase db, string name, long iterator, byte[] data)
        {
            var result = db.Execute(Command.LoadChunk, name, iterator, data);

            return result.ToString() == Keywords.OK;
        }

        private static ICollection<object> BuildArgsForReserve(string name, long initCapacity, double errorRate,
            int? expansion,
            bool? nonScaling)
        {
                var args = new List<object> {name, errorRate, initCapacity};

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
            var args = new List<object> {name};

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
