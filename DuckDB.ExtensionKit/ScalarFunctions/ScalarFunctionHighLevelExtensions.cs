using DuckDB.ExtensionKit.DataChunk.Reader;
using DuckDB.ExtensionKit.DataChunk.Writer;
using DuckDB.ExtensionKit.Extensions;
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

    extension(DuckDBConnection connection)
    {
        public void RegisterScalarFunction<TResult>(string name, Func<TResult> func)
        {
            connection.RegisterScalarFunction<TResult>(name, (writer, rowCount) =>
            {
                for (ulong index = 0; index < rowCount; index++)
                {
                    writer.WriteValue(func(), index);
                }
            });
        }

        public void RegisterScalarFunction<T, TResult>(string name, Func<T, TResult> func)
        {
            var (nullability, anyNullable) = InferParameterNullability(func);

            connection.RegisterScalarFunction<T, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T>(readers[0], index, nullability[0], anyNullable))), new() { HandlesNulls = anyNullable });
        }

        public void RegisterScalarFunction<T, TResult>(string name, Func<T[], TResult> func)
        {
            var elementNullable = InferArrayElementNullability(func);

            connection.RegisterScalarFunction<T, TResult>(name,
                WrapVarargsScalarFunction(func, elementNullable), new() { HandlesNulls = elementNullable }, @params: true);
        }

        public void RegisterScalarFunction<T1, T2, TResult>(string name, Func<T1, T2, TResult> func)
        {
            var (nullability, anyNullable) = InferParameterNullability(func);

            connection.RegisterScalarFunction<T1, T2, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T1>(readers[0], index, nullability[0], anyNullable), ReadValue<T2>(readers[1], index, nullability[1], anyNullable))),
                new() { HandlesNulls = anyNullable });
        }

        public void RegisterScalarFunction<T1, T2, T3, TResult>(string name, Func<T1, T2, T3, TResult> func)
        {
            var (nullability, anyNullable) = InferParameterNullability(func);

            connection.RegisterScalarFunction<T1, T2, T3, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T1>(readers[0], index, nullability[0], anyNullable), ReadValue<T2>(readers[1], index, nullability[1], anyNullable),
                     ReadValue<T3>(readers[2], index, nullability[2], anyNullable))),
                new() { HandlesNulls = anyNullable });
        }

        public void RegisterScalarFunction<T1, T2, T3, T4, TResult>(string name, Func<T1, T2, T3, T4, TResult> func)
        {
            var (nullability, anyNullable) = InferParameterNullability(func);

            connection.RegisterScalarFunction<T1, T2, T3, T4, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T1>(readers[0], index, nullability[0], anyNullable), ReadValue<T2>(readers[1], index, nullability[1], anyNullable),
                     ReadValue<T3>(readers[2], index, nullability[2], anyNullable), ReadValue<T4>(readers[3], index, nullability[3], anyNullable))),
                new() { HandlesNulls = anyNullable });
        }
    }

    // checksNulls: true when any parameter in the function is nullable (special handling active).
    // Needed because set_special_handling is function-level — DuckDB sends NULLs for ALL params,
    // so non-nullable params must also check and throw a descriptive error.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T ReadValue<T>(IDuckDBDataReader reader, ulong index, bool isNullable, bool checksNulls)
    {
        if (checksNulls && !reader.IsValid(index))
        {
            if (isNullable) return default!;
            ThrowNullReceivedByNonNullableParam<T>();
        }

        return typeof(T) == typeof(object) ? (T)reader.GetValue(index) : reader.GetValue<T>(index);
    }

    [DoesNotReturn]
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

    private static Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> WrapVarargsScalarFunction<T, TResult>(Func<T[], TResult> func, bool elementNullable)
    {
        return (readers, writer, rowCount) =>
        {
            var args = new T[readers.Count];

            for (ulong index = 0; index < rowCount; index++)
            {
                for (int r = 0; r < readers.Count; r++)
                {
                    args[r] = ReadValue<T>(readers[r], index, elementNullable, elementNullable);
                }

                writer.WriteValue(func(args), index);
            }
        };
    }

    private static (bool[] perParam, bool anyNullable) InferParameterNullability(Delegate func)
    {
        var context = new NullabilityInfoContext();
        var parameters = func.Method.GetParameters();
        var result = parameters.Select(info => IsNullableParameter(context, info)).ToArray();

        return (result, result.Any(static x => x));
    }

    private static bool InferArrayElementNullability(Delegate func)
    {
        var parameter = func.Method.GetParameters()[0];
        var elementType = parameter.ParameterType.GetElementType();

        // Nullable<T> value types: detectable without attributes
        if (elementType != null && Nullable.GetUnderlyingType(elementType) != null)
            return true;

        // Reference types: check nullable annotation
        var context = new NullabilityInfoContext();
        var info = context.Create(parameter);
        return info.ElementType?.ReadState == NullabilityState.Nullable;
    }

    private static bool IsNullableParameter(NullabilityInfoContext context, ParameterInfo parameter)
    {
        // Nullable<T> value types are always nullable
        if (Nullable.GetUnderlyingType(parameter.ParameterType) != null)
            return true;

        // Reference types: check nullable annotation
        var info = context.Create(parameter);
        return info.ReadState == NullabilityState.Nullable;
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
