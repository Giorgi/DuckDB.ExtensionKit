namespace DuckDB.ExtensionKit.Native;

// The connection the registration code linked from DuckDB.NET passes to NativeMethods, where DuckDB.NET has a
// SafeHandle of the same name. The extension does not own the connection: DuckDB gives it to the entry point.
internal sealed class DuckDBNativeConnection(IntPtr handle)
{
    public IntPtr Handle { get; } = handle;
}
