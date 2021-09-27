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
        public static async Task<bool> CuckooFilterReserveAsync(this IDatabase db, string key, long capacity,
            long? bucketSize = null,
            long? maxIterations = null, long? expansion = null)
        {
            var args = BuildArgsForReserve(key, capacity, bucketSize, maxIterations, expansion);

            var result = await db.ExecuteAsync(Command.Reserve, args);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Adds an item to the cuckoo filter, creating the filter if it does not exist
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfadd">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">The name of the filter</param>
        /// <param name="item">The item to add</param>
        /// <returns><code>true</code> on success, otherwise <code>false</code></returns>
        public static async Task<bool> CuckooFilterAddAsync(this IDatabase db, string key, string item)
        {
            var result = await db.ExecuteAsync(Command.Add, key, item);

            return (bool)result;
        }

        /// <summary>
        /// Adds an item to a cuckoo filter if the item did not exist previously
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfaddnx">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">The name of the filter</param>
        /// <param name="item">The item to add</param>
        /// <returns><code>true</code> item was added to the filter, <code>false</code> if the item already exists</returns>
        public static async Task<bool> CuckooFilterAddAdvancedAsync(this IDatabase db, string key, string item)
        {
            var result = await db.ExecuteAsync(Command.AddAdvanced, key, item);

            return (bool)result;
        }

        /// <summary>
        /// Adds one or more items to a cuckoo filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinsert">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">The name of the filter</param>
        /// <param name="items">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether corresponding item is inserted.
        /// </returns>
        public static async Task<bool[]> CuckooFilterInsertAsync(this IDatabase db, string key, params string[] items)
        {
            var args = BuildArgsForInsert(key, null, items);

            var result = await db.ExecuteAsync(Command.Insert, args);

            return (bool[])result;
        }

        /// <summary>
        /// Adds one or more items to a cuckoo filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinsert">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">The name of the filter</param>
        /// <param name="options"><see cref="InsertOptions"/></param>
        /// <param name="items">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether corresponding item is inserted.
        /// </returns>
        public static async Task<bool[]> CuckooFilterInsertAsync(this IDatabase db, string key, InsertOptions options,
            params string[] items)
        {
            var args = BuildArgsForInsert(key, options, items);

            var result = await db.ExecuteAsync(Command.Insert, args);

            return (bool[])result;
        }

        /// <summary>
        /// Adds one or more items to a cuckoo filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinsertnx">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">The name of the filter</param>
        /// <param name="items">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether corresponding item is inserted.
        /// </returns>
        public static async Task<bool[]> CuckooFilterInsertAdvancedAsync(this IDatabase db, string key, params string[] items)
        {
            var args = BuildArgsForInsert(key, null, items);

            var result = await db.ExecuteAsync(Command.InsertAdvanced, args);

            return (bool[])result;
        }

        /// <summary>
        /// Adds one or more items to a cuckoo filter
        /// <a href="https://oss.redis.com/redisbloom/Cuckoo_Commands/#cfinsertnx">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">The name of the filter</param>
        /// <param name="options"><see cref="InsertOptions"/></param>
        /// <param name="items">Items to add to the filter</param>
        /// <returns>
        /// An array of booleans of the same length as the number of values.
        /// Each boolean values indicates whether corresponding item is inserted.
        /// </returns>
        public static async Task<bool[]> CuckooFilterInsertAdvancedAsync(this IDatabase db, string key, InsertOptions options,
            params string[] items)
        {
            var args = BuildArgsForInsert(key, options, items);

            var result = await db.ExecuteAsync(Command.InsertAdvanced, args);

            return (bool[])result;
        }
    }
}
