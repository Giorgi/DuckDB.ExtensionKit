using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.NET.Data.Functions;
using DuckDB.ExtensionKit.Native;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace DuckDB.ExtensionKit.ScalarFunctions;

public static class ScalarFunctionHighLevelExtensions
{
    /// <summary>
    /// Ensures AOT compiler generates native code for GetValue&lt;T&gt; and WriteValue&lt;T&gt;
    /// for all DuckDB-supported value types. NullableHandler uses MakeGenericMethod to call
    /// GetValidValue&lt;TUnderlying&gt; — the underlying type instantiation must survive trimming
    /// even when only the nullable variant (e.g. int?) is used directly.
    /// </summary>
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Pre-generating AOT code for all supported DuckDB value types")]
    static ScalarFunctionHighLevelExtensions()
    {
        IDuckDBDataReader r = new NoOpDataReader();
        _ = r.GetValue<bool>(0);
        _ = r.GetValue<sbyte>(0);
        _ = r.GetValue<byte>(0);
        _ = r.GetValue<short>(0);
        _ = r.GetValue<ushort>(0);
        _ = r.GetValue<int>(0);
        _ = r.GetValue<uint>(0);
        _ = r.GetValue<long>(0);
        _ = r.GetValue<ulong>(0);
        _ = r.GetValue<float>(0);
        _ = r.GetValue<double>(0);
        _ = r.GetValue<decimal>(0);
        _ = r.GetValue<DateTime>(0);
        _ = r.GetValue<DateOnly>(0);
        _ = r.GetValue<TimeOnly>(0);
        _ = r.GetValue<DateTimeOffset>(0);
        _ = r.GetValue<TimeSpan>(0);
        _ = r.GetValue<Guid>(0);
        _ = r.GetValue<BigInteger>(0);

        IDuckDBDataWriter w = new NoOpDataWriter();
        w.WriteValue<bool>(false, 0);
        w.WriteValue<sbyte>(0, 0);
        w.WriteValue<byte>(0, 0);
        w.WriteValue<short>(0, 0);
        w.WriteValue<ushort>(0, 0);
        w.WriteValue<int>(0, 0);
        w.WriteValue<uint>(0, 0);
        w.WriteValue<long>(0, 0);
        w.WriteValue<ulong>(0, 0);
        w.WriteValue<float>(0, 0);
        w.WriteValue<double>(0, 0);
        w.WriteValue<decimal>(0, 0);
        w.WriteValue<DateTime>(default, 0);
        w.WriteValue<DateOnly>(default, 0);
        w.WriteValue<TimeOnly>(default, 0);
        w.WriteValue<DateTimeOffset>(default, 0);
        w.WriteValue<TimeSpan>(TimeSpan.Zero, 0);
        w.WriteValue<Guid>(Guid.Empty, 0);
        w.WriteValue<BigInteger>(default, 0);
    }

    // The typed functions themselves are DuckDB.NET's (external/DuckDB.NET/DuckDB.NET.Data/Functions/TypedScalarFunctions.cs),
    // compiled into this project. These overloads are the same as DuckDB.NET's DuckDBConnectionScalarFunctionExtensions.
    extension(DuckDBConnection connection)
    {
        public void RegisterScalarFunction<TResult>(string name, Func<TResult> func)
            => TypedScalarFunctions.Register(connection.NativeConnection, name, func);

        public void RegisterScalarFunction<T, TResult>(string name, Func<T, TResult> func)
            => TypedScalarFunctions.Register(connection.NativeConnection, name, func);

        public void RegisterScalarFunction<T, TResult>(string name, Func<T[], TResult> func)
            => TypedScalarFunctions.RegisterVarargs(connection.NativeConnection, name, func);

        public void RegisterScalarFunction<T1, T2, TResult>(string name, Func<T1, T2, TResult> func)
            => TypedScalarFunctions.Register(connection.NativeConnection, name, func);

        public void RegisterScalarFunction<T1, T2, T3, TResult>(string name, Func<T1, T2, T3, TResult> func)
            => TypedScalarFunctions.Register(connection.NativeConnection, name, func);

        public void RegisterScalarFunction<T1, T2, T3, T4, TResult>(string name, Func<T1, T2, T3, T4, TResult> func)
            => TypedScalarFunctions.Register(connection.NativeConnection, name, func);
    }

    private sealed class NoOpDataReader : IDuckDBDataReader
    {
        public Type ClrType => typeof(object);
        public DuckDBType DuckDBType => DuckDBType.Invalid;
        public bool IsValid(ulong offset) => false;
        public T GetValue<T>(ulong offset) => default!;
        public object GetValue(ulong offset) => null!;
    }

    private sealed class NoOpDataWriter : IDuckDBDataWriter
    {
        public void WriteNull(ulong rowIndex) { }
        public void WriteValue<T>(T value, ulong rowIndex) { }
    }
}
