using System.Numerics;
using System.Runtime.InteropServices;


namespace DuckDB.Extension;


// ============================================================================
// Extension Access Struct (passed by DuckDB to entry point)
// ============================================================================

[StructLayout(LayoutKind.Sequential)]
public unsafe struct DuckDBExtensionAccess
{
    public delegate* unmanaged[Cdecl]<nint, byte*, void> SetError;
    public delegate* unmanaged[Cdecl]<nint, nint*> GetDatabase;
    public delegate* unmanaged[Cdecl]<nint, byte*, nint> GetApi;
}

// ============================================================================
// Enums
// ============================================================================

public enum DuckDBState
{
    Success = 0,
    Error = 1
}

public enum DuckDBType : uint
{
    Invalid = 0,
    Boolean = 1,
    TinyInt = 2,
    SmallInt = 3,
    Integer = 4,
    BigInt = 5,
    UnsignedTinyInt = 6,
    UnsignedSmallInt = 7,
    UnsignedInteger = 8,
    UnsignedBigInt = 9,
    Float = 10,
    Double = 11,
    Timestamp = 12,
    Date = 13,
    Time = 14,
    Interval = 15,
    HugeInt = 16,
    UnsignedHugeInt = 32,
    Varchar = 17,
    Blob = 18,
    Decimal = 19,
    TimestampS = 20,
    TimestampMs = 21,
    TimestampNs = 22,
    Enum = 23,
    List = 24,
    Struct = 25,
    Map = 26,
    Array = 33,
    Uuid = 27,
    Union = 28,
    Bit = 29,
    TimeTz = 30,
    TimestampTz = 31,
    VarInt = 34,
    SqlNull = 35,
    Any = 36
}

public enum DuckDBStatementType : int
{
    Invalid = 0,
    Select = 1,
    Insert = 2,
    Update = 3,
    Explain = 4,
    Delete = 5,
    Prepare = 6,
    Create = 7,
    Execute = 8,
    Alter = 9,
    Transaction = 10,
    Copy = 11,
    Analyze = 12,
    VariableSet = 13,
    CreateFunc = 14,
    Drop = 15,
    Export = 16,
    Pragma = 17,
    Vacuum = 18,
    Call = 19,
    Set = 20,
    Load = 21,
    Relation = 22,
    Extension = 23,
    LogicalPlan = 24,
    Attach = 25,
    Detach = 26,
    Multi = 27
}

public enum DuckDBPendingState : uint
{
    ResultReady = 0,
    ResultNotReady = 1,
    Error = 2,
    NoTasksAvailable = 3
}

public enum DuckDBResultType : int
{
    Invalid = 0,
    ChangedRows = 1,
    Nothing = 2,
    QueryResult = 3
}

public enum DuckDBErrorType : int
{
    Invalid = 0,
    OutOfRange = 1,
    Conversion = 2,
    Unknown = 3,
    Serialization = 4,
    Transaction = 5,
    NotImplemented = 6,
    Expression = 7,
    Catalog = 8,
    Parser = 9,
    Planner = 10,
    Scheduler = 11,
    Executor = 12,
    Constraint = 13,
    Index = 14,
    Stat = 15,
    Connection = 16,
    Syntax = 17,
    Settings = 18,
    Binder = 19,
    Network = 20,
    Optimizer = 21,
    NullPointer = 22,
    IO = 23,
    Interrupt = 24,
    Fatal = 25,
    Internal = 26,
    InvalidConfiguration = 27,
    MissingExtension = 28,
    Http = 29,
    AutoLoad = 30,
    SequenceMismatch = 31,
    ParameterNotResolved = 32,
    InvalidType = 33,
    InvalidInput = 34,
    Dependency = 35
}

public enum DuckDBCastMode : int
{
    Normal = 0,
    TryCast = 1
}

// ============================================================================
// Value Structs
// ============================================================================

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBDate
{
    public int Days;
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBTime
{
    public long Micros;
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBTimestamp
{
    public long Micros;
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBTimestampS
{
    public long Seconds;
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBTimestampMs
{
    public long Millis;
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBTimestampNs
{
    public long Nanos;
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBTimeTz
{
    public ulong Bits;
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBInterval
{
    public int Months;
    public int Days;
    public long Micros;
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBHugeInt
{
    private static readonly BigInteger Base = BigInteger.Pow(2, 64);

    public static BigInteger HugeIntMinValue { get; } = BigInteger.Parse("-170141183460469231731687303715884105727");
    public static BigInteger HugeIntMaxValue { get; } = BigInteger.Parse("170141183460469231731687303715884105727");

    public DuckDBHugeInt(BigInteger value)
    {
        if (value < HugeIntMinValue || value > HugeIntMaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(value), $"value must be between {HugeIntMinValue} and {HugeIntMaxValue}");
        }

        var upper = (long)BigInteger.DivRem(value, Base, out var rem);

        if (rem < 0)
        {
            rem += Base;
            upper -= 1;
        }

        Upper = upper;
        Lower = (ulong)rem;
    }

    public DuckDBHugeInt(ulong lower, long upper)
    {
        Lower = lower;
        Upper = upper;
    }

    public ulong Lower { get; }
    public long Upper { get; }

    public BigInteger ToBigInteger()
    {
        return Upper * BigInteger.Pow(2, 64) + Lower;
    }
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBUHugeInt
{
    public ulong Lower;
    public ulong Upper;

    public BigInteger ToBigInteger()
    {
        return Upper * BigInteger.Pow(2, 64) + Lower;
    }
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBDecimal
{
    public byte Width;
    public byte Scale;
    public DuckDBHugeInt Value;
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBDateOnly
{
    public int Year;
    public byte Month;
    public byte Day;
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBTimeOnly
{
    public byte Hour;
    public byte Min;
    public byte Sec;
    public int Micros;
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBTimeTzStruct
{
    public DuckDBTimeOnly Time;
    public int Offset;
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBTimestampStruct
{
    public DuckDBDateOnly Date;
    public DuckDBTimeOnly Time;
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBQueryProgress
{
    public double Percentage;
    public ulong RowsProcessed;
    public ulong TotalRowsToProcess;
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBListEntry(ulong offset, ulong length)
{
    public ulong Offset { get; private set; } = offset;
    public ulong Length { get; private set; } = length;
}

//[StructLayout(LayoutKind.Explicit, Size = 16)]
public unsafe struct DuckDBStringT
{
    public _value_e__Union value;

    private const int InlineStringMaxLength = 12;

    public readonly int Length => (int)value.inlined.length;

    public readonly unsafe sbyte* Data
    {
        get
        {
            if (Length <= InlineStringMaxLength)
            {
                fixed (sbyte* pointerToFirst = value.inlined.inlined)
                {
                    return pointerToFirst;
                }
            }
            else
            {
                return value.pointer.ptr;
            }
        }
    }

    [StructLayout(LayoutKind.Explicit)]
    public partial struct _value_e__Union
    {
        [FieldOffset(0)]
        public DuckDBStringPointer pointer;

        [FieldOffset(0)]
        public DuckDBStringInlined inlined;

        public unsafe partial struct DuckDBStringPointer
        {
            public uint length;

            public fixed sbyte prefix[4];

            public sbyte* ptr;
        }

        public unsafe partial struct DuckDBStringInlined
        {
            public uint length;

            public fixed sbyte inlined[12];
        }
    }
}

[StructLayout(LayoutKind.Sequential)]
public unsafe struct DuckDBBlob
{
    public byte* Data;
    public ulong Size;
}

[StructLayout(LayoutKind.Sequential)]
public unsafe struct DuckDBVarInt
{
    public ulong Size;
    public byte* Data;
}

[StructLayout(LayoutKind.Sequential)]
public unsafe struct DuckDBBit
{
    public ulong Size;
    public byte* Data;
}