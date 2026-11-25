using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.NET.Data.Functions;
using DuckDB.ExtensionKit.Native;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Numerics;

namespace DuckDB.ExtensionKit.TableFunctions;

public static class TableFunctionHighLevelExtensions
{
    /// <summary>
    /// Ensures AOT compiler generates native code for WriteValue&lt;T&gt; and GetValue&lt;T&gt;
    /// for all DuckDB-supported value types. MakeGenericMethod requires pre-compiled
    /// instantiations under Native AOT. Calls go through the interfaces so the compiler
    /// emits code for all implementations.
    /// </summary>
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Pre-generating AOT code for all supported DuckDB value types")]
    static TableFunctionHighLevelExtensions()
    {
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

        IDuckDBValueReader r = new NoOpValueReader();
        _ = r.GetValue<bool>();
        _ = r.GetValue<sbyte>();
        _ = r.GetValue<byte>();
        _ = r.GetValue<short>();
        _ = r.GetValue<ushort>();
        _ = r.GetValue<int>();
        _ = r.GetValue<uint>();
        _ = r.GetValue<long>();
        _ = r.GetValue<ulong>();
        _ = r.GetValue<float>();
        _ = r.GetValue<double>();
        _ = r.GetValue<decimal>();
        _ = r.GetValue<DateTime>();
        _ = r.GetValue<DateOnly>();
        _ = r.GetValue<TimeOnly>();
        _ = r.GetValue<DateTimeOffset>();
        _ = r.GetValue<TimeSpan>();
        _ = r.GetValue<Guid>();
        _ = r.GetValue<BigInteger>();
    }

    // The typed functions themselves are DuckDB.NET's (external/DuckDB.NET/DuckDB.NET.Data/Functions/TypedTableFunctions.cs),
    // compiled into this project. These overloads are the same as DuckDB.NET's DuckDBConnectionTableFunctionExtensions.
    extension(DuckDBConnection connection)
    {
        public void RegisterTableFunction<TData, TProjection>(
            string name,
            Func<IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
            => TypedTableFunctions.Register(connection.NativeConnection, name, dataFunc, projection);

        public void RegisterTableFunction<T1, TData, TProjection>(
            string name,
            Func<T1, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
            => TypedTableFunctions.Register(connection.NativeConnection, name, dataFunc, projection);

        public void RegisterTableFunction<T1, T2, TData, TProjection>(
            string name,
            Func<T1, T2, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
            => TypedTableFunctions.Register(connection.NativeConnection, name, dataFunc, projection);

        public void RegisterTableFunction<T1, T2, T3, TData, TProjection>(
            string name,
            Func<T1, T2, T3, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
            => TypedTableFunctions.Register(connection.NativeConnection, name, dataFunc, projection);

        public void RegisterTableFunction<T1, T2, T3, T4, TData, TProjection>(
            string name,
            Func<T1, T2, T3, T4, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
            => TypedTableFunctions.Register(connection.NativeConnection, name, dataFunc, projection);
    }

    private sealed class NoOpDataWriter : IDuckDBDataWriter
    {
        public void WriteNull(ulong rowIndex) { }
        public void WriteValue<T>(T value, ulong rowIndex) { }
    }

    private sealed class NoOpValueReader : IDuckDBValueReader
    {
        public bool IsNull() => true;
        public T GetValue<T>() => default!;
    }
}
