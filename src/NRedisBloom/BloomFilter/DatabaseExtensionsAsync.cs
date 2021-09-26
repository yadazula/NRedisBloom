using System.Threading.Tasks;
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
        public static async Task<bool> BloomFilterReserveAsync(this IDatabase db, string name, long initCapacity, double errorRate,
            int? expansion = null, bool? nonScaling = null)
        {
            var args = BuildArgsForReserve(name, initCapacity, errorRate, expansion, nonScaling);

            var result = await db.ExecuteAsync(Command.Reserve, args);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Adds an item to the filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfadd">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="name">Name of the filter</param>
        /// <param name="value">Value to add to the filter</param>
        /// <returns>true if the item was not previously in the filter</returns>
        public static async Task<bool> BloomFilterAddAsync(this IDatabase db, string name, string value)
        {
            var result = await db.ExecuteAsync(Command.Add, name, value);

            return (bool)result;
        }

        /// <summary>
        /// Add one or more items to a filter
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
        public static async Task<bool[]> BloomFilterAddMultipleAsync(this IDatabase db, string name, params string[] values)
        {
            var result = await db.ExecuteAsync(Command.AddMultiple, name.PrependToArray(values));

            return (bool[]) result;
        }

        /// <summary>
        /// Add one or more items to the bloom filter, by default creating it if it does not yet exist
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
        public static async Task<bool[]> BloomFilterInsertAsync(this IDatabase db, string name, InsertOptions options,
            params string[] values)
        {
            var args = BuildArgsForInsert(name, options, values);

            var result = await db.ExecuteAsync(Command.Insert, args);

            return (bool[]) result;
        }

        /// <summary>
        /// Check if an item exists in the filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfexists">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="name">Name of the filter</param>
        /// <param name="value">Value to check for</param>
        /// <returns>true if the item may exist in the filter, false if the item does not exist in the filter</returns>
        public static async Task<bool> BloomFilterExistsAsync(this IDatabase db, string name, string value)
        {
            var result = await db.ExecuteAsync(Command.Exists, name, value);

            return (bool) result;
        }

        /// <summary>
        /// Check if one or more items exist in the filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfmexists">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="name">Name of the filter</param>
        /// <param name="values">Values to check for</param>
        /// <returns>An array of booleans. A <code>true</code> value means the corresponding value may exist, <code>false</code> means it does not exist</returns>
        public static async Task<bool[]> BloomFilterExistsMultipleAsync(this IDatabase db, string name, params string[] values)
        {
            var result = await db.ExecuteAsync(Command.ExistsMultiple, name.PrependToArray(values));

            return (bool[]) result;
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
        public static async Task<bool> BloomFilterLoadChunkAsync(this IDatabase db, string name, long iterator, byte[] data)
        {
            var result = await db.ExecuteAsync(Command.LoadChunk, name, iterator, data);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Begins an incremental save of the bloom filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfscandump">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="name">Name of the filter</param>
        /// <param name="iterator">Iterator value; either 0 or the iterator from a previous invocation of this command</param>
        /// <returns>An pair of Iterator and Data. If Iterator is 0, then it means iteration has completed.</returns>
        public static async Task<(long Iterator, byte[] Data)> BloomFilterScanDumpAsync(this IDatabase db, string name, long iterator)
        {
            var result = (RedisResult[]) await db.ExecuteAsync(Command.ScanDump, name, iterator);

            return ((long) result[0], (byte[])result[1]);
        }

        /// <summary>
        /// Get information about the filter
        /// <a href="https://oss.redis.com/redisbloom/Bloom_Commands/#bfinfo">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="name">Name of the filter</param>
        /// <returns>An instance of <see cref="InfoResult"/> that contains information about the filter</returns>
        public static async Task<InfoResult> BloomFilterInfoAsync(this IDatabase db, string name)
        {
            var result = await db.ExecuteAsync(Command.Info, name);

            return InfoResult.Create((RedisResult[]) result);
        }
    }
}
