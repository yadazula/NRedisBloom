using NRedisBloom.Shared;
using StackExchange.Redis;

namespace NRedisBloom.CuckooFilter
{
    /// <summary>
    /// Describes a cuckoo filter
    /// </summary>
    public class InfoResult
    {
        /// <summary>
        /// Size of the cuckoo filter
        /// </summary>
        public long Size { get; private set; }

        /// <summary>
        /// Count of buckets
        /// </summary>
        public long NumberOfBuckets { get; private set; }

        /// <summary>
        /// Count of sub-filters
        /// </summary>
        public int NumberOfFilters { get; private set; }

        /// <summary>
        /// Count of inserted items
        /// </summary>
        public long NumberOfItemsInserted { get; private set; }

        /// <summary>
        /// Count of deleted items
        /// </summary>
        public long NumberOfItemsDeleted { get; private set; }

        /// <summary>
        /// Number of items in each bucket
        /// </summary>
        public long BucketSize { get; private set; }

        /// <summary>
        /// Expansion rate
        /// </summary>
        public int ExpansionRate { get; private set; }

        /// <summary>
        /// Number of attempts to swap items between buckets before declaring filter as full and creating an additional filter
        /// </summary>
        public long MaxIterations { get; private set; }

        internal static InfoResult Create(RedisResult[] redisResult)
        {
            var result = new InfoResult();

            for (var i = 0; i < redisResult.Length; i++)
            {
                var label = (string) redisResult[i];

                switch (label)
                {
                    case Keywords.Size:
                        result.Size = (long) redisResult[++i];
                        break;
                    case Keywords.NumberOfBuckets:
                        result.NumberOfBuckets = (long) redisResult[++i];
                        break;
                    case Keywords.NumberOfFilters:
                        result.NumberOfFilters = (int) redisResult[++i];
                        break;
                    case Keywords.NumberOfItemsInserted:
                        result.NumberOfItemsInserted = (long) redisResult[++i];
                        break;
                    case Keywords.NumberOfItemsDeleted:
                        result.NumberOfItemsDeleted = (long) redisResult[++i];
                        break;
                    case Keywords.BucketSize2:
                        result.BucketSize = (long) redisResult[++i];
                        break;
                    case Keywords.ExpansionRate:
                        result.ExpansionRate = (int) redisResult[++i];
                        break;
                    case Keywords.MaxIterations2:
                        result.MaxIterations = (long) redisResult[++i];
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
