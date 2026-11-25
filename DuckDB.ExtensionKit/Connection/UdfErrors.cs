namespace DuckDB.NET.Data.Connection;

// The registration code linked from DuckDB.NET (external/DuckDB.NET/DuckDB.NET.Data/Functions) calls UdfErrors when a
// function throws. DuckDB.NET's UdfErrors keeps the exception so that the command that ran the query throws it as
// the InnerException. An extension has no .NET caller to hand it to, and DuckDB still gets the message of the
// exception, so nothing is done here. The methods are DuckDB.NET's, with the same signatures.
internal static class UdfErrors
{
    public static void ConfigureScalarFunction(IntPtr function)
    {
    }

    public static void ScalarFunctionFailed(IntPtr info, Exception exception)
    {
    }

    public static ulong TableFunctionBound(IntPtr bindInfo) => 0;

    public static void TableFunctionBindFailed(IntPtr bindInfo, Exception exception)
    {
    }

    public static void TableFunctionFailed(ulong connectionId, Exception exception)
    {
    }
}
