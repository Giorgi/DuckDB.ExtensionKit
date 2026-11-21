using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.NET.Data.Functions;
using DuckDB.ExtensionKit.Native;

namespace DuckDB.ExtensionKit.TableFunctions;

// The registration itself is DuckDB.NET's (external/DuckDB.NET/DuckDB.NET.Data/Functions/TableFunctionRegistration.cs),
// compiled into this project. These methods are the same as DuckDB.NET's DuckDBConnection.RegisterTableFunction.
public static class TableFunctionExtensions
{
    public static void RegisterTableFunction(this DuckDBConnection connection, string name, Func<TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, _ => resultCallback(), mapperCallback);

    public static void RegisterTableFunction<T>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T));

    public static void RegisterTableFunction<T1, T2>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2));

    public static void RegisterTableFunction<T1, T2, T3>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3));

    public static void RegisterTableFunction<T1, T2, T3, T4>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4));

    public static void RegisterTableFunction<T1, T2, T3, T4, T5>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5));

    public static void RegisterTableFunction<T1, T2, T3, T4, T5, T6>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6));

    public static void RegisterTableFunction<T1, T2, T3, T4, T5, T6, T7>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6), typeof(T7));

    public static void RegisterTableFunction<T1, T2, T3, T4, T5, T6, T7, T8>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6), typeof(T7), typeof(T8));

    private static void RegisterTableFunctionInternal(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback, params Type[] parameterTypes)
    {
        var logicalTypes = Array.ConvertAll(parameterTypes, TypeExtensions.GetLogicalType);
        TableFunctionRegistration.Register(connection.NativeConnection, name, (positional, _) => resultCallback(positional), mapperCallback, logicalTypes, []);
    }
}
