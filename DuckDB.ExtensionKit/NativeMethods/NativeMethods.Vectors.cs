using DuckDB.ExtensionKit.Native;

namespace DuckDB.NET.Native;

public static partial class NativeMethods
{
    internal static unsafe class Vectors
    {
        internal static DuckDBLogicalType DuckDBVectorGetColumnType(IntPtr vector) => new(Api.duckdb_vector_get_column_type(vector));

        internal static void* DuckDBVectorGetData(IntPtr vector) => Api.duckdb_vector_get_data(vector);

        internal static ulong* DuckDBVectorGetValidity(IntPtr vector) => Api.duckdb_vector_get_validity(vector);

        internal static void DuckDBVectorEnsureValidityWritable(IntPtr vector) => Api.duckdb_vector_ensure_validity_writable(vector);

        internal static void DuckDBVectorAssignStringElement(IntPtr vector, ulong index, byte* value) => Api.duckdb_vector_assign_string_element(vector, index, value);

        internal static void DuckDBVectorAssignStringElementLength(IntPtr vector, ulong index, byte* value, long length) =>
            Api.duckdb_vector_assign_string_element_len(vector, index, value, (ulong)length);

        // DuckDB.NET writes a string it encoded itself with duckdb_unsafe_vector_assign_string_element_len, which skips
        // DuckDB's check of the UTF-8. That function is in the extension API only from DuckDB v1.5.6 on, so the extension
        // calls the checked one. The bytes are the same: Encoding.UTF8 always writes valid UTF-8.
        internal static void DuckDBUnsafeVectorAssignStringElementLength(IntPtr vector, ulong index, byte* value, long length) =>
            Api.duckdb_vector_assign_string_element_len(vector, index, value, (ulong)length);

        internal static IntPtr DuckDBListVectorGetChild(IntPtr vector) => Api.duckdb_list_vector_get_child(vector);

        internal static ulong DuckDBListVectorGetSize(IntPtr vector) => Api.duckdb_list_vector_get_size(vector);

        internal static DuckDBState DuckDBListVectorSetSize(IntPtr vector, ulong size) => Api.duckdb_list_vector_set_size(vector, size);

        internal static DuckDBState DuckDBListVectorReserve(IntPtr vector, ulong requiredCapacity) => Api.duckdb_list_vector_reserve(vector, requiredCapacity);

        internal static IntPtr DuckDBArrayVectorGetChild(IntPtr vector) => Api.duckdb_array_vector_get_child(vector);

        internal static IntPtr DuckDBStructVectorGetChild(IntPtr vector, long index) => Api.duckdb_struct_vector_get_child(vector, (ulong)index);
    }
}
