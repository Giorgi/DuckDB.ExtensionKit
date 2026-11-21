namespace DuckDB.NET.Native;

// The native calls of the extension. DuckDB hands an extension a table of function pointers (DuckDBExtApiV1), and
// every wrapper here calls through it.
//
// The classes are in DuckDB.NET.Native, with the same names as in DuckDB.NET.Bindings, because the code linked from
// DuckDB.NET (the vector readers and writers, the date and number types) calls NativeMethods.Vectors,
// NativeMethods.LogicalType, ... by those names. A wrapper that linked code calls must have the signature that
// DuckDB.NET.Bindings gives it: the build of this project is what checks it.
public static partial class NativeMethods
{
    private static int _initialized;

    internal static DuckDBExtApiV1 Api;

    public static ref readonly DuckDBExtApiV1 DuckDBApi => ref Api;

    public static void InitializeApi(DuckDBExtApiV1 api)
    {
        if (Interlocked.CompareExchange(ref _initialized, 1, 0) != 0)
        {
            return;
        }

        Api = api;
    }
}
