using System.Collections.Generic;
using System.Threading.Tasks;
using NRedisBloom.Shared;
using StackExchange.Redis;

namespace NRedisBloom.TopK
{
    /// <summary>
    /// Implements RedisBloom TopK Filter commands
    /// </summary>
    public class TopKClient
    {
        private readonly IDatabase _db;

        /// <summary>
        /// Creates a new client for TopK Filter
        /// </summary>
        public TopKClient(IDatabase db) => _db = db;

        /// <summary>
        /// Initializes a TopK Filter with specified parameters
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkreserve">Command Reference</a>
        /// </summary>
        /// <param name="key">Key under which the sketch is to be found</param>
        /// <param name="topk">Number of top occurring items to keep</param>
        /// <param name="width">Number of counters kept in each array</param>
        /// <param name="depth">Number of arrays</param>
        /// <param name="decay">The probability of reducing a counter in an occupied bucket</param>
        /// <returns><code>true</code> if TopK is created</returns>
        public bool Reserve(string key, long topk, long? width = null, long? depth = null,
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

            var result = _db.Execute(Command.Reserve, args);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Initializes a TopK Filter with specified parameters
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkreserve">Command Reference</a>
        /// </summary>
        /// <param name="key">Key under which the sketch is to be found</param>
        /// <param name="topk">Number of top occurring items to keep</param>
        /// <param name="width">Number of counters kept in each array</param>
        /// <param name="depth">Number of arrays</param>
        /// <param name="decay">The probability of reducing a counter in an occupied bucket</param>
        /// <returns><code>true</code> if TopK is created</returns>
        public async Task<bool> ReserveAsync(string key, long topk, long? width = null,
            long? depth = null,
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

            var result = await _db.ExecuteAsync(Command.Reserve, args);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Adds one or more items to a filter
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkadd">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of sketch where item is added</param>
        /// <param name="items">Item(s) to be added</param>
        /// <returns>null if no change to Top-K list occurred else, returns item(s) dropped from list</returns>
        public string[] Add(string key, params string[] items)
        {
            var result = _db.Execute(Command.Add, key.PrependToArray(items));

            return (string[])result;
        }

        /// <summary>
        /// Adds one or more items to a filter
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkadd">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of sketch where item is added</param>
        /// <param name="items">Item(s) to be added</param>
        /// <returns>null if no change to Top-K list occurred else, returns item(s) dropped from list</returns>
        public async Task<string[]> AddAsync(string key, params string[] items)
        {
            var result = await _db.ExecuteAsync(Command.Add, key.PrependToArray(items));

            return (string[])result;
        }

        /// <summary>
        /// Increase the score of an item in the data structure by increment
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkincrby">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of sketch where item is added</param>
        /// <param name="item">Item to be added</param>
        /// <param name="increment">Increment to current item score</param>
        /// <returns>null if no change to Top-K list occurred else, returns item dropped from list</returns>
        public string IncrementBy(string key, string item, long increment)
        {
            var result = _db.Execute(Command.IncrementBy, key, item, increment);

            return (string)result;
        }

        /// <summary>
        /// Increase the score of an item in the data structure by increment
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkincrby">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of sketch where item is queried</param>
        /// <param name="item">Item to be added</param>
        /// <param name="increment">Increment to current item score</param>
        /// <returns>null if no change to Top-K list occurred else, returns item dropped from list</returns>
        public async Task<string> IncrementByAsync(string key, string item,
            long increment)
        {
            var result = await _db.ExecuteAsync(Command.IncrementBy, key, item, increment);

            return (string)result;
        }

        /// <summary>
        /// Checks whether one or more items are one of TopK items
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkquery">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of sketch where item is queried</param>
        /// <param name="items">Item(s) to be queried</param>
        /// <returns>For each item requested, return <code>true</code> if item is in TopK, otherwise <code>false</code></returns>
        public bool[] Query(string key, params string[] items)
        {
            var result = _db.Execute(Command.Query, key.PrependToArray(items));

            return (bool[])result;
        }

        /// <summary>
        /// Checks whether one or more items are one of Top-K items
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkquery">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of sketch where item is added</param>
        /// <param name="items">Item(s) to be queried</param>
        /// <returns>For each item requested, return <code>true</code> if item is in Top-K, otherwise <code>false</code></returns>
        public async Task<bool[]> QueryAsync(string key, params string[] items)
        {
            var result = await _db.ExecuteAsync(Command.Query, key.PrependToArray(items));

            return (bool[])result;
        }

        /// <summary>
        /// Returns count for one or more items
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkcount">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of sketch where item is counted</param>
        /// <param name="items"> Item(s) to be counted</param>
        /// <returns>For each item requested, count for item</returns>
        public long[] Count(string key, params string[] items)
        {
            var result = _db.Execute(Command.Count, key.PrependToArray(items));

            return (long[])result;
        }

        /// <summary>
        /// Returns count for one or more items
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkcount">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of sketch where item is counted</param>
        /// <param name="items"> Item(s) to be counted</param>
        /// <returns>For each item requested, count for item</returns>
        public async Task<long[]> CountAsync(string key, params string[] items)
        {
            var result = await _db.ExecuteAsync(Command.Count, key.PrependToArray(items));

            return (long[])result;
        }

        /// <summary>
        /// Return full list of items in TopK list
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topklist">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of sketch where item is counted</param>
        /// <returns>k (or less) items in TopK list</returns>
        public string[] List(string key)
        {
            var result = _db.Execute(Command.List, key);

            return (string[])result;
        }

        /// <summary>
        /// Returns number of required items (k), width, depth and decay values
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkinfo">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of sketch</param>
        /// <returns>An instance of <see cref="InfoResult"/> that contains information about the TopK</returns>
        public InfoResult Info(string key)
        {
            var result = _db.Execute(Command.Info, key);

            return InfoResult.Create((RedisResult[])result);
        }

        /// <summary>
        /// Return full list of items in TopK list
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topklist">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of sketch where item is counted</param>
        /// <returns>k (or less) items in TopK list</returns>
        public async Task<string[]> ListAsync(string key)
        {
            var result = await _db.ExecuteAsync(Command.List, key);

            return (string[])result;
        }

        /// <summary>
        /// Returns number of required items (k), width, depth and decay values
        /// <a href="https://oss.redis.com/redisbloom/TopK_Commands/#topkinfo">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of sketch</param>
        /// <returns>An instance of <see cref="InfoResult"/> that contains information about the TopK</returns>
        public async Task<InfoResult> InfoAsync(string key)
        {
            var result = await _db.ExecuteAsync(Command.Info, key);

            return InfoResult.Create((RedisResult[])result);
        }
    }
}
