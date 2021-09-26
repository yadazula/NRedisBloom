using NRedisBloom.Shared;
using StackExchange.Redis;

namespace NRedisBloom.TopK
{
    /// <summary>
    /// Describes a Top K filter
    /// </summary>
    public class InfoResult
    {
        /// <summary>
        /// Number of top occurring items
        /// </summary>
        public long K { get; private set; }

        /// <summary>
        /// Number of counters kept in each array
        /// </summary>
        public long Width { get; private set; }

        /// <summary>
        /// Number of arrays
        /// </summary>
        public long Depth { get; private set; }

        /// <summary>
        /// The probability of reducing a counter in an occupied bucket
        /// </summary>
        public double Decay { get; private set; }

        internal static InfoResult Create(RedisResult[] redisResult)
        {
            var result = new InfoResult();

            for (var i = 0; i < redisResult.Length; i++)
            {
                var label = (string) redisResult[i];

                switch (label)
                {
                    case Keywords.TopK:
                        result.K = (long) redisResult[++i];
                        break;
                    case Keywords.Width:
                        result.Width = (long) redisResult[++i];
                        break;
                    case Keywords.Depth:
                        result.Depth = (long) redisResult[++i];
                        break;
                    case Keywords.Decay:
                        result.Decay = (double) redisResult[++i];
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
