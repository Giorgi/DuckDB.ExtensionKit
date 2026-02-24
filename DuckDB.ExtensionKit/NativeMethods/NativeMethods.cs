namespace DuckDB.ExtensionKit.NativeMethods;

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
