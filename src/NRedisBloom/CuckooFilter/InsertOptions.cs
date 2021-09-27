namespace NRedisBloom.CuckooFilter
{
    /// <summary>
    /// Specifies the options for insert command
    /// </summary>
    public class InsertOptions
    {
        /// <summary>
        /// Specifies the desired capacity of the new filter, if this filter does not exist yet
        /// </summary>
        public long? Capacity { get; set; }

        /// <summary>
        ///  If specified, prevents automatic filter creation if the filter does not exist
        /// </summary>
        public bool? NoCreate { get; set; }
    }
}
