namespace NRedisBloom.CountMinSketch
{
    internal static class Command
    {
        internal const string InitByDim = "CMS.INITBYDIM";
        internal const string InitByProb = "CMS.INITBYPROB";
        internal const string IncrBy = "CMS.INCRBY";
        internal const string Query = "CMS.QUERY";
        internal const string Merge = "CMS.MERGE";
        internal const string Info = "CMS.INFO";
    }
}
