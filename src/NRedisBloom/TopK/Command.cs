namespace NRedisBloom.TopK
{
    internal static class Command
    {
        internal const string Reserve = "TOPK.RESERVE";
        internal const string Add = "TOPK.ADD";
        internal const string IncrementBy = "TOPK.INCRBY";
        internal const string Query = "TOPK.QUERY";
        internal const string Count = "TOPK.COUNT";
        internal const string List = "TOPK.LIST";
        internal const string Info = "TOPK.INFO";
    }
}
