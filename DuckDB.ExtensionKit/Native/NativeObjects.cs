using System.Runtime.InteropServices;

namespace DuckDB.ExtensionKit.Native;

// The native types that only the extension API uses. The types shared with DuckDB.NET (DuckDBType, DuckDBState,
// the date, time and number structs, DuckDBString, ...) are linked from external/DuckDB.NET; see the project file.

[StructLayout(LayoutKind.Sequential)]
public unsafe struct DuckDBExtensionAccess
{
    public delegate* unmanaged[Cdecl]<nint, byte*, void> SetError;
    public delegate* unmanaged[Cdecl]<nint, nint*> GetDatabase;
    public delegate* unmanaged[Cdecl]<nint, byte*, nint> GetApi;
}

public enum DuckDBPendingState : uint
{
    ResultReady = 0,
    ResultNotReady = 1,
    Error = 2,
    NoTasksAvailable = 3
}

public enum DuckDBCastMode : int
{
    Normal = 0,
    TryCast = 1
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
