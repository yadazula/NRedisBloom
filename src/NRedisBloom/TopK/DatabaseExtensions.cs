using System.Collections.Generic;
using NRedisBloom.Shared;
using StackExchange.Redis;

namespace NRedisBloom.TopK
{
    /// <summary>
    /// This class defines the extension methods for <see cref="StackExchange.Redis.IDatabase"/>
    /// that allow for the interaction with the RedisBloom (2.x) module.
    /// </summary>
    public static partial class DatabaseExtensions
    {
        /// <summary>
        /// Initializes a TopK filter with specified parameters
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkreserve">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Key under which the sketch is to be found</param>
        /// <param name="topk">Number of top occurring items to keep</param>
        /// <param name="width">Number of counters kept in each array</param>
        /// <param name="depth">Number of arrays</param>
        /// <param name="decay">The probability of reducing a counter in an occupied bucket</param>
        /// <returns><code>true</code> if TopK is created</returns>
        public static bool TopKReserve(this IDatabase db, string key, long topk, long? width = null, long? depth = null,
            double? decay = null)
        {
            var args = new List<object> { key, topk };

            if (width.HasValue)
            {
                args.Add(width);
            }

            if (depth.HasValue)
            {
                args.Add(depth);
            }

            if (decay.HasValue)
            {
                args.Add(decay);
            }

            var result = db.Execute(Command.Reserve, args);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Adds one or more items to a filter
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkadd">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of sketch where item is added</param>
        /// <param name="items">Item(s) to be added</param>
        /// <returns>null if no change to Top-K list occurred else, returns item(s) dropped from list</returns>
        public static string[] TopKAdd(this IDatabase db, string key, params string[] items)
        {
            var result = db.Execute(Command.Add, key.PrependToArray(items));

            return (string[])result;
        }

        /// <summary>
        /// Increase the score of an item in the data structure by increment
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkincrby">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of sketch where item is added</param>
        /// <param name="item">Item to be added</param>
        /// <param name="increment">Increment to current item score</param>
        /// <returns>null if no change to Top-K list occurred else, returns item dropped from list</returns>
        public static string TopKIncrementBy(this IDatabase db, string key, string item, long increment)
        {
            var result = db.Execute(Command.IncrementBy, key, item, increment);

            return (string)result;
        }

        /// <summary>
        /// Checks whether one or more items are one of Top-K items
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkquery">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of sketch where item is queried</param>
        /// <param name="items">Item(s) to be queried</param>
        /// <returns>For each item requested, return <code>true</code> if item is in Top-K, otherwise <code>false</code></returns>
        public static bool[] TopKQuery(this IDatabase db, string key, params string[] items)
        {
            var result = db.Execute(Command.Query, key.PrependToArray(items));

            return (bool[])result;
        }

        /// <summary>
        /// Returns count for one or more items
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkcount">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of sketch where item is counted</param>
        /// <param name="items"> Item(s) to be counted</param>
        /// <returns>For each item requested, count for item</returns>
        public static long[] TopKCount(this IDatabase db, string key, params string[] items)
        {
            var result = db.Execute(Command.Count, key.PrependToArray(items));

            return (long[])result;
        }

        /// <summary>
        /// Return full list of items in Top K list
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topklist">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of sketch where item is counted</param>
        /// <returns>k (or less) items in Top K list</returns>
        public static string[] TopKList(this IDatabase db, string key)
        {
            var result = db.Execute(Command.List, key);

            return (string[])result;
        }

        /// <summary>
        /// Returns number of required items (k), width, depth and decay values
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkinfo">Command Reference</a>
        /// </summary>
        /// <param name="db">Database instance</param>
        /// <param name="key">Name of sketch</param>
        /// <returns>An instance of <see cref="InfoResult"/> that contains information about the Top K</returns>
        public static InfoResult TopKInfo(this IDatabase db, string key)
        {
            var result = db.Execute(Command.Info, key);

            return InfoResult.Create((RedisResult[]) result);
        }
    }
}
