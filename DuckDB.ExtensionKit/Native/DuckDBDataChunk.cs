using Microsoft.Win32.SafeHandles;

namespace DuckDB.ExtensionKit.Native;

internal class DuckDBDataChunk : SafeHandleZeroOrMinusOneIsInvalid
{
    public DuckDBDataChunk() : base(true)
    {
    }

    public DuckDBDataChunk(IntPtr chunk) : base(false)
    {
        SetHandle(chunk);
    }

    protected override bool ReleaseHandle()
    {
        NativeMethods.DataChunks.DuckDBDestroyDataChunk(handle);
        return true;
    }
}