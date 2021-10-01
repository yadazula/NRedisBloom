using System.Collections.Generic;
using System.Threading.Tasks;
using NRedisBloom.Shared;
using StackExchange.Redis;

namespace NRedisBloom.CuckooFilter
{
    /// <summary>
    /// This class defines the extension methods for <see cref="StackExchange.Redis.IDatabase"/>
    /// that allow for the interaction with the RedisBloom (2.x) module.
    /// </summary>
    public static partial class DatabaseExtensions
    {
        /// <summary>
        /// Create a Cuckoo Filter as key with a single sub-filter for the initial amount of capacity for items
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfreserve">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">The key under which the filter is found</param>
        /// <param name="capacity">Estimated capacity for the filter</param>
        /// <param name="bucketSize">Number of items in each bucket</param>
        /// <param name="maxIterations">Number of attempts to swap items between buckets before declaring filter as full and creating an additional filter</param>
        /// <param name="expansion">When a new filter is created, its size is the size of the current filter multiplied by expansion</param>
        /// <returns><code>true</code> if filter is created</returns>
        public static bool CuckooFilterReserve(this IDatabase db, string key, long capacity, long? bucketSize = null,
            long? maxIterations = null, long? expansion = null)
        {
            var args = BuildArgsForReserve(key, capacity, bucketSize, maxIterations, expansion);

            var result = db.Execute(Command.Reserve, args);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Adds an item to the cuckoo filter, creating the filter if it does not exist
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfadd">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of the filter</param>
        /// <param name="item">Item to add</param>
        /// <returns><code>true</code> on success, otherwise <code>false</code></returns>
        public static bool CuckooFilterAdd(this IDatabase db, string key, string item)
        {
            var result = db.Execute(Command.Add, key, item);

            return (bool)result;
        }

        /// <summary>
        /// Adds an item to a cuckoo filter if the item did not exist previously
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfaddnx">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of the filter</param>
        /// <param name="item">Item to add</param>
        /// <returns><code>true</code> item was added to the filter, <code>false</code> if the item already exists</returns>
        public static bool CuckooFilterAddAdvanced(this IDatabase db, string key, string item)
        {
            var result = db.Execute(Command.AddAdvanced, key, item);

            return (bool)result;
        }

        /// <summary>
        /// Adds one or more items to a cuckoo filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinsert">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of the filter</param>
        /// <param name="items">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether corresponding item is inserted.
        /// </returns>
        public static bool[] CuckooFilterInsert(this IDatabase db, string key, params string[] items)
        {
            var args = BuildArgsForInsert(key, null, items);

            var result = db.Execute(Command.Insert, args);

            return (bool[])result;
        }

        /// <summary>
        /// Adds one or more items to a cuckoo filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinsert">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of the filter</param>
        /// <param name="options"><see cref="InsertOptions"/></param>
        /// <param name="items">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether corresponding item is inserted.
        /// </returns>
        public static bool[] CuckooFilterInsert(this IDatabase db, string key, InsertOptions options,
            params string[] items)
        {
            var args = BuildArgsForInsert(key, options, items);

            var result = db.Execute(Command.Insert, args);

            return (bool[])result;
        }

        /// <summary>
        /// Adds one or more items to a cuckoo filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinsertnx">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of the filter</param>
        /// <param name="items">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether corresponding item is inserted.
        /// </returns>
        public static bool[] CuckooFilterInsertAdvanced(this IDatabase db, string key, params string[] items)
        {
            var args = BuildArgsForInsert(key, null, items);

            var result = db.Execute(Command.InsertAdvanced, args);

            return (bool[])result;
        }

        /// <summary>
        /// Adds one or more items to a cuckoo filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinsertnx">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of the filter</param>
        /// <param name="options"><see cref="InsertOptions"/></param>
        /// <param name="items">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether corresponding item is inserted.
        /// </returns>
        public static bool[] CuckooFilterInsertAdvanced(this IDatabase db, string key, InsertOptions options,
            params string[] items)
        {
            var args = BuildArgsForInsert(key, options, items);

            var result = db.Execute(Command.InsertAdvanced, args);

            return (bool[])result;
        }

        /// <summary>
        /// Check if an item exists in a cuckoo filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfexists">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of the filter</param>
        /// <param name="item">Item to check for</param>
        /// <returns><code>true</code> if the item may exist in the filter, <code>false</code> if the item does not exist in the filter</returns>
        public static bool CuckooFilterExists(this IDatabase db, string key, string item)
        {
            var result = db.Execute(Command.Exists, key, item);

            return (bool)result;
        }

        /// <summary>
        /// Deletes an item once from the filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfdel">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of the filter</param>
        /// <param name="item">Item to delete from the filter</param>
        /// <returns><code>true</code> if the item has been deleted, <code>false</code> if the item was not found</returns>
        public static bool CuckooFilterDelete(this IDatabase db, string key, string item)
        {
            var result = db.Execute(Command.Delete, key, item);

            return (bool)result;
        }

        /// <summary>
        /// Returns the number of times an item may be in the filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfcount">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of the filter</param>
        /// <param name="item">Item to count</param>
        /// <returns>The number of times the item exists in the filter</returns>
        public static long CuckooFilterCount(this IDatabase db, string key, string item)
        {
            var result = db.Execute(Command.Count, key, item);

            return (long)result;
        }

        /// <summary>
        /// Begins an incremental save of the cuckoo filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfscandump">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of the filter</param>
        /// <param name="iterator">Iterator value; either 0 or the iterator from a previous invocation of this command</param>
        /// <returns>An pair of Iterator and Data. If Iterator is 0, then it means iteration has completed.</returns>
        public static (long Iterator, byte[] Data) CuckooFilterScanDump(this IDatabase db, string key, long iterator)
        {
            var result = (RedisResult[]) db.Execute(Command.ScanDump, key, iterator);

            return ((long) result[0], (byte[]) result[1]);
        }

        /// <summary>
        /// Restores a filter previously saved using <see cref="CuckooFilterScanDump"/>.
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfloadchunk">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of the filter</param>
        /// <param name="iterator">Iterator value associated with data (returned by <see cref="CuckooFilterScanDump"/>)</param>
        /// <param name="data">Current data chunk (returned by <see cref="CuckooFilterScanDump"/>)</param>
        /// <returns><code>true</code> if chunk is restored</returns>
        public static bool CuckooFilterLoadChunk(this IDatabase db, string key, long iterator, byte[] data)
        {
            var result = db.Execute(Command.LoadChunk, key, iterator, data);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Gets information about the filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinfo">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of the filter</param>
        /// <returns>An instance of <see cref="InfoResult"/> that contains information about the filter</returns>
        public static InfoResult CuckooFilterInfo(this IDatabase db, string key)
        {
            var result = db.Execute(Command.Info, key);

            return InfoResult.Create((RedisResult[]) result);
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
