namespace DuckDB.ExtensionKit.NativeMethods;

public static partial class NativeMethods
{
    private static bool _initialized;

    internal static DuckDBExtApiV1 Api;

    public static ref readonly DuckDBExtApiV1 DuckDBApi => ref Api;

    public static void InitializeApi(DuckDBExtApiV1 api)
    {
        if (_initialized)
        {
            throw new InvalidOperationException("DuckDB API has already been initialized.");
        }

        Api = api;
        _initialized = true;
    }
}
