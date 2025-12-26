namespace DuckDB.Extension;

internal class DuckDBNativeConnection(IntPtr connection)
{
    public IntPtr Connection { get; } = connection;
}