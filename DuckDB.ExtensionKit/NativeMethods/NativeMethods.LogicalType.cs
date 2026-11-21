using DuckDB.ExtensionKit.Extensions;
using DuckDB.ExtensionKit.Native;

namespace DuckDB.NET.Native;

public static partial class NativeMethods
{
    internal static unsafe class LogicalType
    {
        internal static DuckDBLogicalType DuckDBCreateLogicalType(DuckDBType type) => new(Api.duckdb_create_logical_type(type));

        internal static DuckDBLogicalType DuckDBCreateDecimalType(byte width, byte scale) => new(Api.duckdb_create_decimal_type(width, scale));

        internal static DuckDBLogicalType DuckDBCreateListType(DuckDBLogicalType type) => new(Api.duckdb_create_list_type(type.DangerousGetHandle()));

        internal static DuckDBType DuckDBGetTypeId(DuckDBLogicalType type) => Api.duckdb_get_type_id(type.DangerousGetHandle());

        internal static byte DuckDBDecimalWidth(DuckDBLogicalType type) => Api.duckdb_decimal_width(type.DangerousGetHandle());

        internal static byte DuckDBDecimalScale(DuckDBLogicalType type) => Api.duckdb_decimal_scale(type.DangerousGetHandle());

        internal static DuckDBType DuckDBDecimalInternalType(DuckDBLogicalType type) => Api.duckdb_decimal_internal_type(type.DangerousGetHandle());

        internal static DuckDBLogicalType DuckDBListTypeChildType(DuckDBLogicalType type) => new(Api.duckdb_list_type_child_type(type.DangerousGetHandle()));

        internal static DuckDBLogicalType DuckDBArrayTypeChildType(DuckDBLogicalType type) => new(Api.duckdb_array_type_child_type(type.DangerousGetHandle()));

        // Named after DuckDB.NET.Bindings; the C function is duckdb_array_type_array_size.
        internal static long DuckDBArrayVectorGetSize(DuckDBLogicalType type) => (long)Api.duckdb_array_type_array_size(type.DangerousGetHandle());

        internal static DuckDBLogicalType DuckDBMapTypeKeyType(DuckDBLogicalType type) => new(Api.duckdb_map_type_key_type(type.DangerousGetHandle()));

        internal static DuckDBLogicalType DuckDBMapTypeValueType(DuckDBLogicalType type) => new(Api.duckdb_map_type_value_type(type.DangerousGetHandle()));

        internal static DuckDBType DuckDBEnumInternalType(DuckDBLogicalType type) => Api.duckdb_enum_internal_type(type.DangerousGetHandle());

        internal static uint DuckDBEnumDictionarySize(DuckDBLogicalType type) => Api.duckdb_enum_dictionary_size(type.DangerousGetHandle());

        // The caller owns the string DuckDB returns: ToManagedString frees it, as DuckDBCallerOwnedStringMarshaller does in DuckDB.NET.
        internal static string DuckDBEnumDictionaryValue(DuckDBLogicalType type, long index) =>
            PointerExtensions.ToManagedString(Api.duckdb_enum_dictionary_value(type.DangerousGetHandle(), (ulong)index));

        internal static long DuckDBStructTypeChildCount(DuckDBLogicalType type) => (long)Api.duckdb_struct_type_child_count(type.DangerousGetHandle());

        internal static string DuckDBStructTypeChildName(DuckDBLogicalType type, long index) =>
            PointerExtensions.ToManagedString(Api.duckdb_struct_type_child_name(type.DangerousGetHandle(), (ulong)index));

        internal static DuckDBLogicalType DuckDBStructTypeChildType(DuckDBLogicalType type, long index) =>
            new(Api.duckdb_struct_type_child_type(type.DangerousGetHandle(), (ulong)index));

        internal static void DuckDBDestroyLogicalType(IntPtr type) => Api.duckdb_destroy_logical_type(&type);
    }
}
