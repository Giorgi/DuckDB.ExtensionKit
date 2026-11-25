using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.NET.Data.Functions;
using DuckDB.ExtensionKit.Native;

namespace DuckDB.ExtensionKit.ScalarFunctions;

// The registration itself is DuckDB.NET's (external/DuckDB.NET/DuckDB.NET.Data/Functions/ScalarFunctionRegistration.cs),
// compiled into this project. These methods are the same as DuckDB.NET's DuckDBConnection.RegisterScalarFunction.
public static class ScalarFunctionExtensions
{
    public static void RegisterScalarFunction<TResult>(this DuckDBConnection connection, string name, Action<IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        ScalarFunctionRegistration.Register(connection.NativeConnection, name, (_, w, index) => action(w, index), TypeExtensions.GetLogicalType<TResult>(), varargs: false, options);
    }

    public static void RegisterScalarFunction<T, TResult>(this DuckDBConnection connection, string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null, bool @params = false)
    {
        ScalarFunctionRegistration.Register(connection.NativeConnection, name, action, TypeExtensions.GetLogicalType<TResult>(), @params, options, TypeExtensions.GetLogicalType<T>());
    }

    public static void RegisterScalarFunction<T1, T2, TResult>(this DuckDBConnection connection, string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        ScalarFunctionRegistration.Register(connection.NativeConnection, name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, options,
            TypeExtensions.GetLogicalType<T1>(), TypeExtensions.GetLogicalType<T2>());
    }

    public static void RegisterScalarFunction<T1, T2, T3, TResult>(this DuckDBConnection connection, string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        ScalarFunctionRegistration.Register(connection.NativeConnection, name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, options,
            TypeExtensions.GetLogicalType<T1>(), TypeExtensions.GetLogicalType<T2>(), TypeExtensions.GetLogicalType<T3>());
    }

    public static void RegisterScalarFunction<T1, T2, T3, T4, TResult>(this DuckDBConnection connection, string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        ScalarFunctionRegistration.Register(connection.NativeConnection, name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, options,
            TypeExtensions.GetLogicalType<T1>(), TypeExtensions.GetLogicalType<T2>(),
            TypeExtensions.GetLogicalType<T3>(), TypeExtensions.GetLogicalType<T4>());
    }
}
