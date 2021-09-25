namespace NRedisBloom
{
    internal static class Command
    {
        internal const string Reserve = "BF.RESERVE";
        internal const string Add = "BF.ADD";
        internal const string AddMultiple = "BF.MADD";
        internal const string Exists = "BF.EXISTS";
        internal const string ExistsMultiple = "BF.MEXISTS";
        internal const string Insert = "BF.INSERT";
        internal const string Info = "BF.INFO";
        internal const string ScanDump = "BF.SCANDUMP";
        internal const string LoadChunk = "BF.LOADCHUNK";
    }
}
