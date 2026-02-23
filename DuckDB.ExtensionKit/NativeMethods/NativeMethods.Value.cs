using DuckDB.ExtensionKit.Extensions;
using DuckDB.ExtensionKit.Native;

namespace DuckDB.ExtensionKit.NativeMethods;

public static partial class NativeMethods
{
    internal static unsafe class Value
    {
        internal static void DuckDBDestroyValue(IntPtr value) => Api.duckdb_destroy_value(&value);

        internal static bool DuckDBIsNullValue(DuckDBValue value) => Api.duckdb_is_null_value(value.DangerousGetHandle()) != 0;

        internal static DuckDBLogicalType DuckDBGetValueType(DuckDBValue value) => new(Api.duckdb_get_value_type(value.DangerousGetHandle()));

        // Get primitive values
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

        internal static string DuckDBGetVarchar(DuckDBValue value) => PointerExtensions.ToManagedString(Api.duckdb_get_varchar(value.DangerousGetHandle()));
    }
}
