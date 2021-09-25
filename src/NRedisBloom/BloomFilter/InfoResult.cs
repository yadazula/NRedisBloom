using StackExchange.Redis;

namespace NRedisBloom.BloomFilter
{
    /// <summary>
    /// Describes a bloom filter
    /// </summary>
    public class InfoResult
    {
        /// <summary>
        /// Capacity of the bloom filter
        /// </summary>
        public long Capacity { get; private set; }

        /// <summary>
        /// Size of the bloom filter
        /// </summary>
        public long Size { get; private set; }

        /// <summary>
        /// Count of sub-filters
        /// </summary>
        public int NumberOfFilters { get; private set; }

        /// <summary>
        /// Count of inserted items
        /// </summary>
        public long NumberOfItemsInserted { get; private set; }

        /// <summary>
        /// Expansion rate
        /// </summary>
        public int? ExpansionRate { get; private set; }

        internal static InfoResult Create(RedisResult[] redisResult)
        {
            var result = new InfoResult();

            for (var i = 0; i < redisResult.Length; i++)
            {
                var label = (string) redisResult[i];

                switch (label)
                {
                    case Keywords.Capacity:
                        result.Capacity = (long) redisResult[++i];
                        break;
                    case Keywords.Size:
                        result.Size = (long) redisResult[++i];
                        break;
                    case Keywords.NumberOfFilters:
                        result.NumberOfFilters = (int) redisResult[++i];
                        break;
                    case Keywords.NumberOfItemsInserted:
                        result.NumberOfItemsInserted = (long) redisResult[++i];
                        break;
                    case Keywords.ExpansionRate:
                        result.ExpansionRate = (int?) redisResult[++i];
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
