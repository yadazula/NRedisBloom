using NRedisBloom.Shared;
using StackExchange.Redis;

namespace NRedisBloom.CountMinSketch
{
    /// <summary>
    /// Describes a Count-Min Sketch
    /// </summary>
    public class InfoResult
    {
        /// <summary>
        /// Width of the sketch
        /// </summary>
        public long Width { get; private set; }

        /// <summary>
        /// Depth of the sketch
        /// </summary>
        public long Depth { get; private set; }

        /// <summary>
        /// Total count of the sketch
        /// </summary>
        public long Count { get; private set; }

        internal static InfoResult Create(RedisResult[] redisResult)
        {
            var result = new InfoResult();

            for (var i = 0; i < redisResult.Length; i++)
            {
                var label = (string) redisResult[i];
                switch (label)
                {
                    case Keywords.Width:
                        result.Width = (long) redisResult[++i];
                        break;
                    case Keywords.Depth:
                        result.Depth = (long) redisResult[++i];
                        break;
                    case Keywords.Count:
                        result.Count = (long) redisResult[++i];
                        break;
                    default:
                        ++i;
                        break;
                }
            }

            return result;
        }
    }
}
