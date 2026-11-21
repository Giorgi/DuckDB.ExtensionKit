using DuckDB.ExtensionKit.Extensions;
using DuckDB.ExtensionKit.Native;

namespace DuckDB.NET.Native;

public static partial class NativeMethods
{
    // The signatures of DuckDB.NET.Bindings (NativeMethods.Value.cs), which DuckDBValue, linked from DuckDB.NET, calls.
    internal static unsafe class Value
    {
        internal static void DuckDBDestroyValue(ref IntPtr value)
        {
            fixed (IntPtr* pointer = &value)
            {
                Api.duckdb_destroy_value(pointer);
            }
        }

        internal static bool DuckDBIsNullValue(DuckDBValue value) => Api.duckdb_is_null_value(value.DangerousGetHandle()) != 0;

        internal static DuckDBLogicalType DuckDBGetValueType(DuckDBValue value) => new(Api.duckdb_get_value_type(value.DangerousGetHandle()));

        internal static bool DuckDBGetBool(DuckDBValue value) => Api.duckdb_get_bool(value.DangerousGetHandle()) != 0;

        internal static sbyte DuckDBGetInt8(DuckDBValue value) => Api.duckdb_get_int8(value.DangerousGetHandle());

        internal static short DuckDBGetInt16(DuckDBValue value) => Api.duckdb_get_int16(value.DangerousGetHandle());

        internal static int DuckDBGetInt32(DuckDBValue value) => Api.duckdb_get_int32(value.DangerousGetHandle());

        internal static long DuckDBGetInt64(DuckDBValue value) => Api.duckdb_get_int64(value.DangerousGetHandle());

        internal static byte DuckDBGetUInt8(DuckDBValue value) => Api.duckdb_get_uint8(value.DangerousGetHandle());

        internal static ushort DuckDBGetUInt16(DuckDBValue value) => Api.duckdb_get_uint16(value.DangerousGetHandle());

        internal static uint DuckDBGetUInt32(DuckDBValue value) => Api.duckdb_get_uint32(value.DangerousGetHandle());

        internal static ulong DuckDBGetUInt64(DuckDBValue value) => Api.duckdb_get_uint64(value.DangerousGetHandle());

        internal static float DuckDBGetFloat(DuckDBValue value) => Api.duckdb_get_float(value.DangerousGetHandle());

        internal static double DuckDBGetDouble(DuckDBValue value) => Api.duckdb_get_double(value.DangerousGetHandle());

        internal static DuckDBHugeInt DuckDBGetHugeInt(DuckDBValue value) => Api.duckdb_get_hugeint(value.DangerousGetHandle());

        internal static DuckDBUHugeInt DuckDBGetUHugeInt(DuckDBValue value) => Api.duckdb_get_uhugeint(value.DangerousGetHandle());

        // The caller owns the string: ToManagedString frees it, as DuckDBCallerOwnedStringMarshaller does in DuckDB.NET.
        internal static string DuckDBGetVarchar(DuckDBValue value) => PointerExtensions.ToManagedString(Api.duckdb_get_varchar(value.DangerousGetHandle()));

        internal static DuckDBDate DuckDBGetDate(DuckDBValue value) => Api.duckdb_get_date(value.DangerousGetHandle());

        internal static DuckDBTime DuckDBGetTime(DuckDBValue value) => Api.duckdb_get_time(value.DangerousGetHandle());

        // duckdb_get_time_ns is in the extension C API from v1.5.6 only, and TIME_NS is newer than the DuckDB versions
        // of C API v1.2.0. DuckDBValue calls this for a TIME_NS argument of a table function only.
        internal static DuckDBTimeNs DuckDBGetTimeNs(DuckDBValue value) =>
            throw new NotSupportedException("A TIME_NS argument of a table function cannot be read: duckdb_get_time_ns is not in the C API the extension targets.");

        internal static DuckDBTimeTzStruct DuckDBGetTimeTz(DuckDBValue value) => Api.duckdb_get_time_tz(value.DangerousGetHandle());

        internal static DuckDBInterval DuckDBGetInterval(DuckDBValue value) => Api.duckdb_get_interval(value.DangerousGetHandle());

        internal static DuckDBTimestampStruct DuckDBGetTimestamp(DuckDBValue value) => Api.duckdb_get_timestamp(value.DangerousGetHandle());

        // As in DuckDB.NET, the value comes back in its own unit (seconds, milliseconds, nanoseconds), in the field of
        // a DuckDBTimestampStruct, and DuckDBValue converts it.
        internal static DuckDBTimestampStruct DuckDBGetTimestampS(DuckDBValue value) => new() { Micros = Api.duckdb_get_timestamp_s(value.DangerousGetHandle()).Seconds };

        internal static DuckDBTimestampStruct DuckDBGetTimestampMs(DuckDBValue value) => new() { Micros = Api.duckdb_get_timestamp_ms(value.DangerousGetHandle()).Millis };

        internal static DuckDBTimestampStruct DuckDBGetTimestampNs(DuckDBValue value) => new() { Micros = Api.duckdb_get_timestamp_ns(value.DangerousGetHandle()).Nanos };
    }
}
