using StackExchange.Redis;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NRedisBloom.Shared;

namespace NRedisBloom.CountMinSketch
{
    /// <summary>
    /// Implements RedisBloom Count-Min-Sketch commands
    /// </summary>
    public class CountMinSketchClient
    {
        private readonly IDatabase _db;

        /// <summary>
        /// Creates a new client for Count-Min-Sketch
        /// </summary>
        public CountMinSketchClient(IDatabase db) => _db = db;

        /// <summary>
        /// Initializes a Count-Min Sketch to dimensions specified by user
        /// <a href="https://oss.redis.com/redisbloom/CountMinSketch_Commands/#cmsinitbydim">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the sketch</param>
        /// <param name="width">Number of counter in each array</param>
        /// <param name="depth">Number of counter-arrays</param>
        /// <returns><code>true</code> if count-min sketch is initialized</returns>
        public bool InitByDim(string key, long width, long depth)
        {
            var result = _db.Execute(Command.InitByDim, key, width, depth);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Initializes a Count-Min Sketch to dimensions specified by user
        /// <a href="https://oss.redis.com/redisbloom/CountMinSketch_Commands/#cmsinitbydim">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the sketch</param>
        /// <param name="width">Number of counter in each array</param>
        /// <param name="depth">Number of counter-arrays</param>
        /// <returns><code>true</code> if count-min sketch is initialized</returns>
        public async Task<bool> InitByDimAsync(string key, long width,
            long depth)
        {
            var result = await _db.ExecuteAsync(Command.InitByDim, key, width, depth);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Initializes a Count-Min Sketch to accommodate requested tolerances
        /// <a href="https://oss.redis.com/redisbloom/CountMinSketch_Commands/#cmsinitbyprob">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the sketch</param>
        /// <param name="error">Estimate size of error</param>
        /// <param name="probability">The desired probability for inflated count</param>
        /// <returns><code>true</code> if count-min sketch is initialized</returns>
        public bool InitByProb(string key, double error, double probability)
        {
            var result = _db.Execute(Command.InitByProb, key, error, probability);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Initializes a Count-Min Sketch to accommodate requested tolerances
        /// <a href="https://oss.redis.com/redisbloom/CountMinSketch_Commands/#cmsinitbyprob">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the sketch</param>
        /// <param name="error">Estimate size of error</param>
        /// <param name="probability">The desired probability for inflated count</param>
        /// <returns><code>true</code> if count-min sketch is initialized</returns>
        public async Task<bool> InitByProbAsync(string key, double error,
            double probability)
        {
            var result = await _db.ExecuteAsync(Command.InitByProb, key, error, probability);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Increases the count of item by increment
        /// <a href="https://oss.redis.com/redisbloom/CountMinSketch_Commands/#cmsincrby">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the sketch</param>
        /// <param name="item">Item which counter to be increased</param>
        /// <param name="increment">Amount by which the item counter is to be increased</param>
        /// <returns>Count of item after increment</returns>
        public long IncrBy(string key, string item, long increment)
        {
            var result = _db.Execute(Command.IncrBy, key, item, increment);

            return (long)result;
        }

        /// <summary>
        /// Increases the count of multiple items
        /// <a href="https://oss.redis.com/redisbloom/CountMinSketch_Commands/#cmsincrby">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the sketch</param>
        /// <param name="itemIncrements">Item and increment map which the item counter is to be increased</param>
        /// <returns>Count of each item after increment</returns>
        public long[] IncrBy(string key,
            IDictionary<string, long> itemIncrements)
        {
            var args = new List<object> { key };

            foreach (var itemIncrement in itemIncrements)
            {
                args.Add(itemIncrement.Key);
                args.Add(itemIncrement.Value);
            }

            var result = _db.Execute(Command.IncrBy, args);

            return (long[])result;
        }

        /// <summary>
        /// Increases the count of item by increment
        /// <a href="https://oss.redis.com/redisbloom/CountMinSketch_Commands/#cmsincrby">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the sketch</param>
        /// <param name="item">Item which counter to be increased</param>
        /// <param name="increment">Amount by which the item counter is to be increased</param>
        /// <returns>Count of item after increment</returns>
        public async Task<long> IncrByAsync(string key, string item,
            long increment)
        {
            var result = await _db.ExecuteAsync(Command.IncrBy, key, item, increment);

            return (long)result;
        }

        /// <summary>
        /// Increases the count of multiple items
        /// <a href="https://oss.redis.com/redisbloom/CountMinSketch_Commands/#cmsincrby">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the sketch</param>
        /// <param name="itemIncrements">Item and increment map which the item counter is to be increased</param>
        /// <returns>Count of each item after increment</returns>
        public async Task<long[]> IncrByAsync(string key,
            IDictionary<string, long> itemIncrements)
        {
            var args = new List<object> { key };

            foreach (var itemIncrement in itemIncrements)
            {
                args.Add(itemIncrement.Key);
                args.Add(itemIncrement.Value);
            }

            var result = await _db.ExecuteAsync(Command.IncrBy, args);

            return (long[])result;
        }

        /// <summary>
        /// Returns the count for one or more items in a sketch
        /// <a href="https://oss.redis.com/redisbloom/CountMinSketch_Commands/#cmsquery">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the sketch</param>
        /// <param name="items">One or more items for which to return the count</param>
        /// <returns>Count of one or more items</returns>
        public long[] Query(string key, params string[] items)
        {
            var result = _db.Execute(Command.Query, key.PrependToArray(items));

            return (long[])result;
        }

        /// <summary>
        /// Returns the count for one or more items in a sketch
        /// <a href="https://oss.redis.com/redisbloom/CountMinSketch_Commands/#cmsquery">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the sketch</param>
        /// <param name="items">One or more items for which to return the count</param>
        /// <returns>Count of one or more items</returns>
        public async Task<long[]> QueryAsync(string key, params string[] items)
        {
            var result = await _db.ExecuteAsync(Command.Query, key.PrependToArray(items));

            return (long[]) result;
        }

        /// <summary>
        /// Merges several sketches into one sketch
        /// <a href="https://oss.redis.com/redisbloom/CountMinSketch_Commands/#merge">Command Reference</a>
        /// </summary>
        /// <param name="dest">Name of destination sketch</param>
        /// <param name="keys">Names of source sketches to be merged</param>
        /// <returns><code>true</code> on success</returns>
        public bool Merge(string dest, params string[] keys)
        {
            var args = new List<object> { dest, keys.Length };
            args.AddRange(keys);

            var result = _db.Execute(Command.Merge, args);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Merges several sketches into one sketch
        /// <a href="https://oss.redis.com/redisbloom/CountMinSketch_Commands/#merge">Command Reference</a>
        /// </summary>
        /// <param name="dest">Name of destination sketch</param>
        /// <param name="keysAndWeights"></param>
        /// <returns><code>true</code> on success</returns>
        public bool Merge(string dest,
            IDictionary<string, long> keysAndWeights)
        {
            var args = new List<object> { dest, keysAndWeights.Count };
            args.AddRange(keysAndWeights.Keys);
            args.Add(Keywords.Weights);
            args.AddRange(keysAndWeights.Values.Cast<object>());

            var result = _db.Execute(Command.Merge, args);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Merges several sketches into one sketch
        /// <a href="https://oss.redis.com/redisbloom/CountMinSketch_Commands/#merge">Command Reference</a>
        /// </summary>
        /// <param name="dest">Name of destination sketch</param>
        /// <param name="keys">Names of source sketches to be merged</param>
        /// <returns><code>true</code> on success</returns>
        public async Task<bool> MergeAsync(string dest, params string[] keys)
        {
            var args = new List<object> { dest, keys.Length };
            args.AddRange(keys);

            var result = await _db.ExecuteAsync(Command.Merge, args);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Merges several sketches into one sketch
        /// <a href="https://oss.redis.com/redisbloom/CountMinSketch_Commands/#merge">Command Reference</a>
        /// </summary>
        /// <param name="dest">Name of destination sketch</param>
        /// <param name="keysAndWeights"></param>
        /// <returns><code>true</code> on success</returns>
        public async Task<bool> MergeAsync(string dest,
            IDictionary<string, long> keysAndWeights)
        {
            var args = new List<object> { dest, keysAndWeights.Count };
            args.AddRange(keysAndWeights.Keys);
            args.Add(Keywords.Weights);
            args.AddRange(keysAndWeights.Values.Cast<object>());

            var result = await _db.ExecuteAsync(Command.Merge, args);

            return result.ToString() == Keywords.OK;
        }

        /// <summary>
        /// Gets information about the sketch
        /// <a href="https://oss.redis.com/redisbloom/CountMinSketch_Commands/#cmsinfo">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the sketch</param>
        /// <returns>An instance of <see cref="InfoResult" /> containing the width, depth and total count of the sketch</returns>
        public InfoResult Info(string key)
        {
            var result = _db.Execute(Command.Info, key);

            return InfoResult.Create((RedisResult[])result);
        }

        /// <summary>
        /// Gets information about the sketch
        /// <a href="https://oss.redis.com/redisbloom/CountMinSketch_Commands/#cmsinfo">Command Reference</a>
        /// </summary>
        /// <param name="key">Name of the sketch</param>
        /// <returns>An instance of <see cref="InfoResult" /> containing the width, depth and total count of the sketch</returns>
        public async Task<InfoResult> InfoAsync(string key)
        {
            var result = await _db.ExecuteAsync(Command.Info, key);

            return InfoResult.Create((RedisResult[])result);
        }
    }
}
