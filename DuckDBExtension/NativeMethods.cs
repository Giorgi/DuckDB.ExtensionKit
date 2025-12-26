namespace DuckDB.Extension;

static class NativeMethods
{
    public static DuckDBExtApiV1 Api;

    internal static unsafe class DataChunks
    {
        public static ulong DuckDBDataChunkGetColumnCount(DuckDBDataChunk chunk) =>
            Api.duckdb_data_chunk_get_column_count(chunk.DangerousGetHandle());

        public static IntPtr DuckDBDataChunkGetVector(DuckDBDataChunk chunk, long columnIndex) =>
            Api.duckdb_data_chunk_get_vector(chunk.DangerousGetHandle(), (ulong)columnIndex);

        public static ulong DuckDBDataChunkGetSize(DuckDBDataChunk chunk) =>
            Api.duckdb_data_chunk_get_size(chunk.DangerousGetHandle());

        public static void DuckDBDestroyDataChunk(IntPtr chunk) => Api.duckdb_destroy_data_chunk(&chunk);

        public static void DuckDBDataChunkSetSize(DuckDBDataChunk chunk, ulong size) => Api.duckdb_data_chunk_set_size(chunk.DangerousGetHandle(), size);
    }

    internal static unsafe class LogicalType
    {
        public static DuckDBLogicalType DuckDBCreateLogicalType(DuckDBType type)
        {
            var logicalType = Api.duckdb_create_logical_type(type);

            return new DuckDBLogicalType(logicalType);
        }

        public static DuckDBLogicalType DuckDBCreateDecimalType(byte width, byte scale)
        {
            var type = Api.duckdb_create_decimal_type(width, scale);

            return new DuckDBLogicalType(type);
        }

        public static DuckDBType DuckDBGetTypeId(DuckDBLogicalType type) => Api.duckdb_get_type_id(type.DangerousGetHandle());

        public static byte DuckDBDecimalWidth(DuckDBLogicalType type) => Api.duckdb_decimal_width(type.DangerousGetHandle());

        public static byte DuckDBDecimalScale(DuckDBLogicalType type) => Api.duckdb_decimal_scale(type.DangerousGetHandle());

        public static DuckDBType DuckDBDecimalInternalType(DuckDBLogicalType type) => Api.duckdb_decimal_internal_type(type.DangerousGetHandle());

        public static DuckDBLogicalType DuckDBListTypeChildType(DuckDBLogicalType type)
        {
            var childType = Api.duckdb_list_type_child_type(type.DangerousGetHandle());

            return new DuckDBLogicalType(childType);
        }

        public static DuckDBLogicalType DuckDBArrayTypeChildType(DuckDBLogicalType type)
        {
            var childType = Api.duckdb_array_type_child_type(type.DangerousGetHandle());

            return new DuckDBLogicalType(childType);
        }

        public static ulong DuckDBArrayVectorGetSize(DuckDBLogicalType type) => Api.duckdb_array_type_array_size(type.DangerousGetHandle());

        public static DuckDBLogicalType DuckDBMapTypeKeyType(DuckDBLogicalType type)
        {
            var keyType = Api.duckdb_map_type_key_type(type.DangerousGetHandle());

            return new DuckDBLogicalType(keyType);
        }

        public static DuckDBLogicalType DuckDBMapTypeValueType(DuckDBLogicalType type)
        {
            var valueType = Api.duckdb_map_type_value_type(type.DangerousGetHandle());

            return new DuckDBLogicalType(valueType);
        }

        public static DuckDBType DuckDBEnumInternalType(DuckDBLogicalType type) => Api.duckdb_enum_internal_type(type.DangerousGetHandle());

        public static uint DuckDBEnumDictionarySize(DuckDBLogicalType type) => Api.duckdb_enum_dictionary_size(type.DangerousGetHandle());

        public static byte* DuckDBEnumDictionaryValue(DuckDBLogicalType type, ulong index) => Api.duckdb_enum_dictionary_value(type.DangerousGetHandle(), index);

        public static void DuckDBDestroyLogicalType(IntPtr type) => Api.duckdb_destroy_logical_type(&type);
    }

    internal static unsafe class ScalarFunction
    {
        public static IntPtr DuckDBCreateScalarFunction() =>
            Api.duckdb_create_scalar_function();

        public static void DuckDBDestroyScalarFunction(IntPtr* scalarFunction) =>
            Api.duckdb_destroy_scalar_function(scalarFunction);

        public static void DuckDBScalarFunctionSetName(IntPtr scalarFunction, byte* name) =>
            Api.duckdb_scalar_function_set_name(scalarFunction, name);

        public static void DuckDBScalarFunctionSetVarargs(IntPtr scalarFunction, DuckDBLogicalType type) =>
            Api.duckdb_scalar_function_set_varargs(scalarFunction, type.DangerousGetHandle());

        public static void DuckDBScalarFunctionSetVolatile(IntPtr scalarFunction) =>
            Api.duckdb_scalar_function_set_volatile(scalarFunction);

        public static void DuckDBScalarFunctionAddParameter(IntPtr scalarFunction, DuckDBLogicalType type) =>
            Api.duckdb_scalar_function_add_parameter(scalarFunction, type.DangerousGetHandle());

        public static void DuckDBScalarFunctionSetReturnType(IntPtr scalarFunction, DuckDBLogicalType type) =>
            Api.duckdb_scalar_function_set_return_type(scalarFunction, type.DangerousGetHandle());

        public static void DuckDBScalarFunctionSetExtraInfo(IntPtr scalarFunction, IntPtr extraInfo, delegate* unmanaged[Cdecl]<void*, void> destroy) =>
            Api.duckdb_scalar_function_set_extra_info(scalarFunction, extraInfo.ToPointer(), destroy);

        public static void DuckDBScalarFunctionSetFunction(IntPtr scalarFunction, delegate* unmanaged[Cdecl]<IntPtr, IntPtr, IntPtr, void> callback) =>
            Api.duckdb_scalar_function_set_function(scalarFunction, callback);

        public static DuckDBState DuckDBRegisterScalarFunction(IntPtr connection, IntPtr scalarFunction) =>
            Api.duckdb_register_scalar_function(connection, scalarFunction);

        public static void* DuckDBScalarFunctionGetExtraInfo(IntPtr scalarFunction) =>
            Api.duckdb_scalar_function_get_extra_info(scalarFunction);
    }

    internal static unsafe class TableFunction
    {
        public static IntPtr DuckDBCreateTableFunction() =>
            Api.duckdb_create_table_function();

        public static void DuckDBDestroyTableFunction(IntPtr tableFunction) => Api.duckdb_destroy_table_function(&tableFunction);

        public static void DuckDBTableFunctionSetName(IntPtr tableFunction, byte* name) =>
            Api.duckdb_table_function_set_name(tableFunction, name);

        public static void DuckDBTableFunctionAddParameter(IntPtr tableFunction, DuckDBLogicalType type) =>
            Api.duckdb_table_function_add_parameter(tableFunction, type.DangerousGetHandle());

        public static void DuckDBTableFunctionSetExtraInfo(IntPtr tableFunction, IntPtr extraInfo, delegate* unmanaged[Cdecl]<void*, void> destroy) =>
            Api.duckdb_table_function_set_extra_info(tableFunction, extraInfo.ToPointer(), destroy);

        public static void DuckDBTableFunctionSetBind(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, void> bind) =>
            Api.duckdb_table_function_set_bind(tableFunction, bind);

        public static void DuckDBTableFunctionSetInit(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, void> init) =>
            Api.duckdb_table_function_set_init(tableFunction, init);

        public static void DuckDBTableFunctionSetFunction(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, IntPtr, void> function) =>
            Api.duckdb_table_function_set_function(tableFunction, function);

        public static DuckDBState DuckDBRegisterTableFunction(IntPtr connection, IntPtr tableFunction) =>
            Api.duckdb_register_table_function(connection, tableFunction);

        // Bind methods
        public static void* DuckDBBindGetExtraInfo(IntPtr info) =>
            Api.duckdb_bind_get_extra_info(info);

        public static ulong DuckDBBindGetParameterCount(IntPtr info) =>
            Api.duckdb_bind_get_parameter_count(info);

        public static IntPtr DuckDBBindGetParameter(IntPtr info, ulong index) =>
            Api.duckdb_bind_get_parameter(info, index);

        public static void DuckDBBindAddResultColumn(IntPtr info, byte* name, DuckDBLogicalType type) =>
            Api.duckdb_bind_add_result_column(info, name, type.DangerousGetHandle());

        public static void DuckDBBindSetBindData(IntPtr info, IntPtr bindData, delegate* unmanaged[Cdecl]<void*, void> destroy) =>
            Api.duckdb_bind_set_bind_data(info, bindData.ToPointer(), destroy);

        public static void DuckDBBindSetError(IntPtr info, byte* error) =>
            Api.duckdb_bind_set_error(info, error);

        // Function methods
        public static void* DuckDBFunctionGetBindData(IntPtr info) =>
            Api.duckdb_function_get_bind_data(info);

        public static void* DuckDBFunctionGetExtraInfo(IntPtr info) =>
            Api.duckdb_function_get_extra_info(info);

        public static void DuckDBFunctionSetError(IntPtr info, byte* error) =>
            Api.duckdb_function_set_error(info, error);
    }

    internal static unsafe class Vectors
    {
        public static DuckDBLogicalType DuckDBVectorGetColumnType(IntPtr vector) => new(Api.duckdb_vector_get_column_type(vector));

        public static void* DuckDBVectorGetData(IntPtr vector) => Api.duckdb_vector_get_data(vector);

        public static ulong* DuckDBVectorGetValidity(IntPtr vector) => Api.duckdb_vector_get_validity(vector);

        public static void DuckDBVectorEnsureValidityWritable(IntPtr vector) => Api.duckdb_vector_ensure_validity_writable(vector);

        public static void DuckDBVectorAssignStringElement(IntPtr vector, ulong index, byte* value) => Api.duckdb_vector_assign_string_element(vector, index, value);

        public static void DuckDBVectorAssignStringElementLength(IntPtr vector, ulong index, byte* value, ulong length) => Api.duckdb_vector_assign_string_element_len(vector, index, value, length);

        public static IntPtr DuckDBListVectorGetChild(IntPtr vector) => Api.duckdb_list_vector_get_child(vector);

        public static ulong DuckDBListVectorGetSize(IntPtr vector) => Api.duckdb_list_vector_get_size(vector);

        public static DuckDBState DuckDBListVectorSetSize(IntPtr vector, ulong size) => Api.duckdb_list_vector_set_size(vector, size);

        public static DuckDBState DuckDBListVectorReserve(IntPtr vector, ulong capacity) => Api.duckdb_list_vector_reserve(vector, capacity);

        public static IntPtr DuckDBArrayVectorGetChild(IntPtr vector) => Api.duckdb_array_vector_get_child(vector);

        public static IntPtr DuckDBStructVectorGetChild(IntPtr vector, ulong index) => Api.duckdb_struct_vector_get_child(vector, index);
    }

    internal static unsafe class ValidityMask
    {
        public static bool DuckDBValidityRowIsValid(ulong* validity, ulong row) => Api.duckdb_validity_row_is_valid(validity, row) != 0;

        public static void DuckDBValiditySetRowValidity(ulong* validity, ulong row, bool valid) => Api.duckdb_validity_set_row_validity(validity, row, (byte)(valid ? 1 : 0));

        public static void DuckDBValiditySetRowInvalid(ulong* validity, ulong row) => Api.duckdb_validity_set_row_invalid(validity, row);

        public static void DuckDBValiditySetRowValid(ulong* validity, ulong row) => Api.duckdb_validity_set_row_valid(validity, row);
    }

    public static unsafe class Helpers
    {
        public static void DuckDBFree(byte* ptr) => Api.duckdb_free(ptr);

        public static ulong DuckDBVectorSize() => Api.duckdb_vector_size();
    }

    internal static unsafe class Value
    {
        public static void DuckDBDestroyValue(IntPtr value) => Api.duckdb_destroy_value(&value);

        public static bool DuckDBIsNullValue(DuckDBValue value) => Api.duckdb_is_null_value(value.DangerousGetHandle()) != 0;

        public static DuckDBLogicalType DuckDBGetValueType(DuckDBValue value) => new(Api.duckdb_get_value_type(value.DangerousGetHandle()));

        // Get primitive values
        public static bool DuckDBGetBool(DuckDBValue value) => Api.duckdb_get_bool(value.DangerousGetHandle()) != 0;

        public static sbyte DuckDBGetInt8(DuckDBValue value) => Api.duckdb_get_int8(value.DangerousGetHandle());

        public static short DuckDBGetInt16(DuckDBValue value) => Api.duckdb_get_int16(value.DangerousGetHandle());

        public static int DuckDBGetInt32(DuckDBValue value) => Api.duckdb_get_int32(value.DangerousGetHandle());

        public static long DuckDBGetInt64(DuckDBValue value) => Api.duckdb_get_int64(value.DangerousGetHandle());

        public static byte DuckDBGetUInt8(DuckDBValue value) => Api.duckdb_get_uint8(value.DangerousGetHandle());

        public static ushort DuckDBGetUInt16(DuckDBValue value) => Api.duckdb_get_uint16(value.DangerousGetHandle());

        public static uint DuckDBGetUInt32(DuckDBValue value) => Api.duckdb_get_uint32(value.DangerousGetHandle());

        public static ulong DuckDBGetUInt64(DuckDBValue value) => Api.duckdb_get_uint64(value.DangerousGetHandle());

        public static float DuckDBGetFloat(DuckDBValue value) => Api.duckdb_get_float(value.DangerousGetHandle());

        public static double DuckDBGetDouble(DuckDBValue value) => Api.duckdb_get_double(value.DangerousGetHandle());

        public static DuckDBHugeInt DuckDBGetHugeInt(DuckDBValue value) => Api.duckdb_get_hugeint(value.DangerousGetHandle());

        public static DuckDBUHugeInt DuckDBGetUHugeInt(DuckDBValue value) => Api.duckdb_get_uhugeint(value.DangerousGetHandle());

        public static string DuckDBGetVarchar(DuckDBValue value)
        {
            var ptr = Api.duckdb_get_varchar(value.DangerousGetHandle());
            return System.Runtime.InteropServices.Marshal.PtrToStringUTF8((IntPtr)ptr) ?? string.Empty;
        }
    }
}