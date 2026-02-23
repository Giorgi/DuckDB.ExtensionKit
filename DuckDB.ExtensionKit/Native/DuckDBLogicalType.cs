using Microsoft.Win32.SafeHandles;

namespace DuckDB.ExtensionKit.Native;

internal class DuckDBLogicalType() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    public DuckDBLogicalType(IntPtr handle) : this()
    {
        SetHandle(handle);
    }

    protected override bool ReleaseHandle()
    {
        NativeMethods.NativeMethods.LogicalType.DuckDBDestroyLogicalType(handle);
        return true;
    }
}