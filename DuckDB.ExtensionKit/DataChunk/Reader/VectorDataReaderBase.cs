using DuckDB.ExtensionKit.Native;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace DuckDB.ExtensionKit.DataChunk.Reader;

internal class VectorDataReaderBase : IDisposable, IDuckDBDataReader
{
    private readonly unsafe ulong* validityMaskPointer;

    [field: AllowNull, MaybeNull]
    public Type ClrType => field ??= GetColumnType();

    public string ColumnName { get; }
    public DuckDBType DuckDBType { get; }
    private protected unsafe void* DataPointer { get; }

    internal unsafe VectorDataReaderBase(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName)
    {
        DataPointer = dataPointer;
        this.validityMaskPointer = validityMaskPointer;

        DuckDBType = columnType;
        ColumnName = columnName;
    }

    public unsafe bool IsValid(ulong offset)
    {
        if (validityMaskPointer == default)
        {
            return true;
        }

        var validityMaskEntryIndex = offset / 64;
        var validityBitIndex = (int)(offset % 64);

        var validityMaskEntryPtr = validityMaskPointer + validityMaskEntryIndex;
        var validityBit = 1ul << validityBitIndex;

        var isValid = (*validityMaskEntryPtr & validityBit) != 0;
        return isValid;
    }

    public virtual T GetValue<T>(ulong offset)
    {
        // When T is Nullable<TUnderlying> (e.g. int?), we can't call GetValidValue<int>() directly
        // because we only have T=int? at compile time. NullableHandler uses a pre-compiled expression
        // tree that calls GetValidValue<int>() and converts to int?, avoiding boxing through the
        // non-generic GetValue(offset, Type) path.
        if (NullableHandler<T>.IsNullableValueType)
        {
            return NullableHandler<T>.Read(this, offset);
        }

        if (IsValid(offset))
        {
            return GetValidValue<T>(offset, typeof(T));
        }

        throw new InvalidCastException($"Column '{ColumnName}' value is null");
    }

    /// <summary>
    /// Called when the value at specified <param name="offset">offset</param> is valid (isn't null)
    /// </summary>
    /// <typeparam name="T">Type of the return value</typeparam>
    /// <param name="offset">Position to read the data from</param>
    /// <param name="targetType">Type of the return value</param>
    /// <returns>Data at the specified offset</returns>
    protected virtual T GetValidValue<T>(ulong offset, Type targetType)
    {
        return (T)GetValue(offset, targetType);
    }

    public object GetValue(ulong offset)
    {
        return GetValue(offset, ClrType);
    }

    internal virtual object GetValue(ulong offset, Type targetType)
    {
        return DuckDBType switch
        {
            DuckDBType.Invalid => throw new DuckDBException($"Invalid type for column {ColumnName}"),
            _ => throw new ArgumentException($"Unrecognised type {DuckDBType} ({(int)DuckDBType}) for column {ColumnName}")
        };
    }

    protected virtual Type GetColumnType()
    {
        return DuckDBType switch
        {
            DuckDBType.Invalid => throw new DuckDBException($"Invalid type for column {ColumnName}"),
            DuckDBType.Boolean => typeof(bool),
            DuckDBType.TinyInt => typeof(sbyte),
            DuckDBType.SmallInt => typeof(short),
            DuckDBType.Integer => typeof(int),
            DuckDBType.BigInt => typeof(long),
            DuckDBType.UnsignedTinyInt => typeof(byte),
            DuckDBType.UnsignedSmallInt => typeof(ushort),
            DuckDBType.UnsignedInteger=> typeof(uint),
            DuckDBType.UnsignedBigInt => typeof(ulong),
            DuckDBType.Float => typeof(float),
            DuckDBType.Double => typeof(double),
            DuckDBType.Timestamp => typeof(DateTime),
            DuckDBType.Interval => typeof(TimeSpan),
            DuckDBType.Date => typeof(DateOnly),
            DuckDBType.Time => typeof(TimeOnly),
            DuckDBType.TimeTz => typeof(DateTimeOffset),
            DuckDBType.HugeInt => typeof(BigInteger),
            DuckDBType.UnsignedHugeInt => typeof(BigInteger),
            DuckDBType.Varchar => typeof(string),
            DuckDBType.Decimal => typeof(decimal),
            DuckDBType.TimestampS => typeof(DateTime),
            DuckDBType.TimestampMs => typeof(DateTime),
            DuckDBType.TimestampNs => typeof(DateTime),
            DuckDBType.Blob => typeof(Stream),
            DuckDBType.Enum => typeof(string),
            DuckDBType.Uuid => typeof(Guid),
            DuckDBType.Struct => typeof(Dictionary<string, object>),
            DuckDBType.Bit => typeof(string),
            DuckDBType.TimestampTz => typeof(DateTime),
            DuckDBType.VarInt => typeof(BigInteger),
            _ => throw new ArgumentException($"Unrecognised type {DuckDBType} ({(int)DuckDBType}) for column {ColumnName}")
        };
    }

    protected unsafe T GetFieldData<T>(ulong offset) where T : unmanaged => *((T*)DataPointer + offset);

    public virtual void Dispose()
    {
    }

    private static class NullableHandler<T>
    {
        private static Type type;
        private static Type? underlyingType;

        static NullableHandler()
        {
            type = typeof(T);
            underlyingType = Nullable.GetUnderlyingType(type);
            IsNullableValueType = underlyingType != null;
            Read = IsNullableValueType ? Compile() : null!;
        }

        public static readonly bool IsNullableValueType;
        public static readonly Func<VectorDataReaderBase, ulong, T> Read;

        // For T = int?, builds a delegate equivalent to:
        //   (VectorDataReaderBase reader, ulong offset) =>
        //       reader.IsValid(offset)
        //           ? (int?)reader.GetValidValue<int>(offset, typeof(int))
        //           : default(int?)
        [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "ScalarFunctionHighLevelExtensions cctor pre-generates all supported value type instantiations")]
        private static Func<VectorDataReaderBase, ulong, T> Compile()
        {
            if (underlyingType is null) return null!;

            var reader = Expression.Parameter(typeof(VectorDataReaderBase));
            var offset = Expression.Parameter(typeof(ulong));

            var isValid = Expression.Call(reader, typeof(VectorDataReaderBase).GetMethod(nameof(IsValid))!, offset);

            var methodInfo = typeof(VectorDataReaderBase).GetMethod(nameof(GetValidValue), BindingFlags.Instance | BindingFlags.NonPublic)!;
            var genericGetValidValue = methodInfo.MakeGenericMethod(underlyingType);

            var getValidValue = Expression.Call(reader, genericGetValidValue, offset, Expression.Constant(underlyingType));

            var body = Expression.Condition(isValid, Expression.Convert(getValidValue, type), Expression.Default(type));

            return Expression.Lambda<Func<VectorDataReaderBase, ulong, T>>(body, reader, offset).Compile();
        }
    }
}