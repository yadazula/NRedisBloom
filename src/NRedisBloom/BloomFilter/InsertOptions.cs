namespace NRedisBloom.BloomFilter
{
    /// <summary>
    /// Specifies the options for insert command
    /// </summary>
    public class InsertOptions
    {
        /// <summary>
        /// If specified, should be followed by the desired capacity for the filter to be created
        /// </summary>
        public long? Capacity { get; set; }

        /// <summary>
        /// If specified, should be followed by the the error ratio of the newly created filter if it does not yet exist
        /// </summary>
        public double? ErrorRate { get; set; }

        /// <summary>
        /// When capacity is reached, an additional sub-filter is created.
        /// The size of the new sub-filter is the size of the last sub-filter multiplied by expansion
        /// </summary>
        public int? Expansion { get; set; }

        /// <summary>
        /// If specified, indicates that the filter should not be created if it does not already exist
        /// It is an error to specify NOCREATE together with either CAPACITY or ERROR.
        /// </summary>
        public bool? NoCreate { get; set; }

        /// <summary>
        /// Prevents the filter from creating additional sub-filters if initial capacity is reached
        /// </summary>
        public bool? NonScaling { get; set; }
    }
}
