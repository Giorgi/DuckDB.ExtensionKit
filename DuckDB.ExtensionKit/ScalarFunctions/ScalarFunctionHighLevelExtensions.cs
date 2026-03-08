using DuckDB.ExtensionKit.DataChunk.Reader;
using DuckDB.ExtensionKit.DataChunk.Writer;
using DuckDB.ExtensionKit.Extensions;
using DuckDB.ExtensionKit.Native;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
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

    extension(DuckDBConnection connection)
    {
        public void RegisterScalarFunction<TResult>(string name, Func<TResult> func, ScalarFunctionOptions? options = null)
        {
            connection.RegisterScalarFunction<TResult>(name, (writer, rowCount) =>
            {
                for (ulong index = 0; index < rowCount; index++)
                {
                    writer.WriteValue(func(), index);
                }
            }, options);
        }

        public void RegisterScalarFunction<T, TResult>(string name, Func<T, TResult> func, ScalarFunctionOptions? options = null)
        {
            ValidateHandlesNulls(options, typeof(T));
            var handlesNulls = options?.HandlesNulls ?? false;

            connection.RegisterScalarFunction<T, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T>(readers[0], index, handlesNulls))), options);
        }

        public void RegisterScalarFunction<T, TResult>(string name, Func<T[], TResult> func, ScalarFunctionOptions? options = null)
        {
            ValidateHandlesNulls(options, typeof(T));
            var handlesNulls = options?.HandlesNulls ?? false;

            connection.RegisterScalarFunction<T, TResult>(name,
                WrapVarargsScalarFunction(func, handlesNulls), options, @params: true);
        }

        public void RegisterScalarFunction<T1, T2, TResult>(string name, Func<T1, T2, TResult> func, ScalarFunctionOptions? options = null)
        {
            ValidateHandlesNulls(options, typeof(T1), typeof(T2));
            var handlesNulls = options?.HandlesNulls ?? false;

            connection.RegisterScalarFunction<T1, T2, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T1>(readers[0], index, handlesNulls), ReadValue<T2>(readers[1], index, handlesNulls))), options);
        }

        public void RegisterScalarFunction<T1, T2, T3, TResult>(string name, Func<T1, T2, T3, TResult> func, ScalarFunctionOptions? options = null)
        {
            ValidateHandlesNulls(options, typeof(T1), typeof(T2), typeof(T3));
            var handlesNulls = options?.HandlesNulls ?? false;

            connection.RegisterScalarFunction<T1, T2, T3, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T1>(readers[0], index, handlesNulls), ReadValue<T2>(readers[1], index, handlesNulls),
                     ReadValue<T3>(readers[2], index, handlesNulls))), options);
        }

        public void RegisterScalarFunction<T1, T2, T3, T4, TResult>(string name, Func<T1, T2, T3, T4, TResult> func, ScalarFunctionOptions? options = null)
        {
            ValidateHandlesNulls(options, typeof(T1), typeof(T2), typeof(T3), typeof(T4));
            var handlesNulls = options?.HandlesNulls ?? false;

            connection.RegisterScalarFunction<T1, T2, T3, T4, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T1>(readers[0], index, handlesNulls), ReadValue<T2>(readers[1], index, handlesNulls),
                     ReadValue<T3>(readers[2], index, handlesNulls), ReadValue<T4>(readers[3], index, handlesNulls))), options);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T ReadValue<T>(IDuckDBDataReader reader, ulong index, bool handlesNulls)
    {
        if (typeof(T) == typeof(object))
        {
            if (handlesNulls && !reader.IsValid(index)) return default!;
            return (T)reader.GetValue(index);
        }

        if (handlesNulls && !reader.IsValid(index))
        {
            if (default(T) is null) return default!;
            ThrowNullReceivedByNonNullableParam<T>();
        }

        return reader.GetValue<T>(index);
    }

    private static void ThrowNullReceivedByNonNullableParam<T>()
    {
        throw new InvalidOperationException(
            $"Scalar function parameter of type '{typeof(T).Name}' received NULL. " +
            $"Use '{typeof(T).Name}?' to handle NULL values.");
    }

    private static Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> WrapScalarFunction<TResult>(Func<IReadOnlyList<IDuckDBDataReader>, ulong, TResult> perRowFunc)
    {
        return (readers, writer, rowCount) =>
        {
            for (ulong index = 0; index < rowCount; index++)
            {
                var result = perRowFunc(readers, index);
                writer.WriteValue(result, index);
            }
        };
    }

    private static Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> WrapVarargsScalarFunction<T, TResult>(Func<T[], TResult> func, bool handlesNulls)
    {
        return (readers, writer, rowCount) =>
        {
            var args = new T[readers.Count];

            for (ulong index = 0; index < rowCount; index++)
            {
                for (int r = 0; r < readers.Count; r++)
                {
                    args[r] = ReadValue<T>(readers[r], index, handlesNulls);
                }

                writer.WriteValue(func(args), index);
            }
        };
    }

    //If HandlesNulls is true, at least one parameter type must be nullable to allow null values to be passed in.
    private static void ValidateHandlesNulls(ScalarFunctionOptions? options, params Type[] parameterTypes)
    {
        if (options?.HandlesNulls == true && !parameterTypes.Any(t => t.AllowsNullValue(out _, out _)))
            throw new ArgumentException("HandlesNulls requires at least one nullable parameter type (use int? instead of int).");
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
