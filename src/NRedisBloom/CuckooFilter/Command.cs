namespace NRedisBloom.CuckooFilter
{
    internal static class Command
    {
        internal const string Reserve = "CF.RESERVE";
        internal const string Add = "CF.ADD";
        internal const string AddAdvanced = "CF.ADDNX";
        internal const string Insert = "CF.INSERT";
        internal const string InsertAdvanced = "CF.INSERTNX";
        internal const string Exists = "CF.EXISTS";
        internal const string Delete = "CF.DEL";
        internal const string Count = "CF.COUNT";
        internal const string ScanDump = "CF.SCANDUMP";
        internal const string LoadChunk = "CF.LOADCHUNK";
        internal const string Info = "CF.INFO";
    }
}
