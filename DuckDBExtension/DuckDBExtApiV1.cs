using System.Runtime.InteropServices;

namespace DuckDB.Extension;

[StructLayout(LayoutKind.Sequential)]
public unsafe struct DuckDBExtApiV1
{
    // idx_t = ulong, size_t = nuint, bool = byte in C ABI

    // --- Database/Connection ---
    public delegate* unmanaged[Cdecl]<byte*, nint*, DuckDBState> duckdb_open;
    public delegate* unmanaged[Cdecl]<byte*, nint*, nint, byte**, DuckDBState> duckdb_open_ext;
    public delegate* unmanaged[Cdecl]<nint*, void> duckdb_close;
    public delegate* unmanaged[Cdecl]<nint, nint*, DuckDBState> duckdb_connect;
    public delegate* unmanaged[Cdecl]<nint, void> duckdb_interrupt;
    public delegate* unmanaged[Cdecl]<nint, DuckDBQueryProgress> duckdb_query_progress;
    public delegate* unmanaged[Cdecl]<nint*, void> duckdb_disconnect;
    public delegate* unmanaged[Cdecl]<byte*> duckdb_library_version;

    // --- Config ---
    public delegate* unmanaged[Cdecl]<nint*, DuckDBState> duckdb_create_config;
    public delegate* unmanaged[Cdecl]<nuint> duckdb_config_count;
    public delegate* unmanaged[Cdecl]<nuint, byte**, byte**, DuckDBState> duckdb_get_config_flag;
    public delegate* unmanaged[Cdecl]<nint, byte*, byte*, DuckDBState> duckdb_set_config;
    public delegate* unmanaged[Cdecl]<nint*, void> duckdb_destroy_config;

    // --- Query ---
    public delegate* unmanaged[Cdecl]<nint, byte*, nint*, DuckDBState> duckdb_query;
    public delegate* unmanaged[Cdecl]<nint*, void> duckdb_destroy_result;
    public delegate* unmanaged[Cdecl]<nint*, ulong, byte*> duckdb_column_name;
    public delegate* unmanaged[Cdecl]<nint*, ulong, DuckDBType> duckdb_column_type;
    public delegate* unmanaged[Cdecl]<nint, DuckDBStatementType> duckdb_result_statement_type;
    public delegate* unmanaged[Cdecl]<nint*, ulong, nint> duckdb_column_logical_type;
    public delegate* unmanaged[Cdecl]<nint*, ulong> duckdb_column_count;
    public delegate* unmanaged[Cdecl]<nint*, ulong> duckdb_rows_changed;
    public delegate* unmanaged[Cdecl]<nint*, byte*> duckdb_result_error;
    public delegate* unmanaged[Cdecl]<nint*, DuckDBErrorType> duckdb_result_error_type;
    public delegate* unmanaged[Cdecl]<nint, DuckDBResultType> duckdb_result_return_type;

    // --- Memory ---
    public delegate* unmanaged[Cdecl]<nuint, void*> duckdb_malloc;
    public delegate* unmanaged[Cdecl]<void*, void> duckdb_free;
    public delegate* unmanaged[Cdecl]<ulong> duckdb_vector_size;

    // --- String helpers ---
    public delegate* unmanaged[Cdecl]<DuckDBStringT, byte> duckdb_string_is_inlined;
    public delegate* unmanaged[Cdecl]<DuckDBStringT, uint> duckdb_string_t_length;
    public delegate* unmanaged[Cdecl]<DuckDBStringT*, byte*> duckdb_string_t_data;

    // --- Date/Time conversions ---
    public delegate* unmanaged[Cdecl]<DuckDBDate, DuckDBDateOnly> duckdb_from_date;
    public delegate* unmanaged[Cdecl]<DuckDBDateOnly, DuckDBDate> duckdb_to_date;
    public delegate* unmanaged[Cdecl]<DuckDBDate, byte> duckdb_is_finite_date;
    public delegate* unmanaged[Cdecl]<DuckDBTime, DuckDBTimeOnly> duckdb_from_time;
    public delegate* unmanaged[Cdecl]<long, int, DuckDBTimeTz> duckdb_create_time_tz;
    public delegate* unmanaged[Cdecl]<DuckDBTimeTz, DuckDBTimeTzStruct> duckdb_from_time_tz;
    public delegate* unmanaged[Cdecl]<DuckDBTimeOnly, DuckDBTime> duckdb_to_time;
    public delegate* unmanaged[Cdecl]<DuckDBTimestamp, DuckDBTimestampStruct> duckdb_from_timestamp;
    public delegate* unmanaged[Cdecl]<DuckDBTimestampStruct, DuckDBTimestamp> duckdb_to_timestamp;
    public delegate* unmanaged[Cdecl]<DuckDBTimestamp, byte> duckdb_is_finite_timestamp;

    // --- Numeric conversions ---
    public delegate* unmanaged[Cdecl]<DuckDBHugeInt, double> duckdb_hugeint_to_double;
    public delegate* unmanaged[Cdecl]<double, DuckDBHugeInt> duckdb_double_to_hugeint;
    public delegate* unmanaged[Cdecl]<DuckDBUHugeInt, double> duckdb_uhugeint_to_double;
    public delegate* unmanaged[Cdecl]<double, DuckDBUHugeInt> duckdb_double_to_uhugeint;
    public delegate* unmanaged[Cdecl]<double, byte, byte, DuckDBDecimal> duckdb_double_to_decimal;
    public delegate* unmanaged[Cdecl]<DuckDBDecimal, double> duckdb_decimal_to_double;

    // --- Prepared Statements ---
    public delegate* unmanaged[Cdecl]<nint, byte*, nint*, DuckDBState> duckdb_prepare;
    public delegate* unmanaged[Cdecl]<nint*, void> duckdb_destroy_prepare;
    public delegate* unmanaged[Cdecl]<nint, byte*> duckdb_prepare_error;
    public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_nparams;
    public delegate* unmanaged[Cdecl]<nint, ulong, byte*> duckdb_parameter_name;
    public delegate* unmanaged[Cdecl]<nint, ulong, DuckDBType> duckdb_param_type;
    public delegate* unmanaged[Cdecl]<nint, ulong, nint> duckdb_param_logical_type;
    public delegate* unmanaged[Cdecl]<nint, DuckDBState> duckdb_clear_bindings;
    public delegate* unmanaged[Cdecl]<nint, DuckDBStatementType> duckdb_prepared_statement_type;

    // --- Bind functions ---
    public delegate* unmanaged[Cdecl]<nint, ulong, nint, DuckDBState> duckdb_bind_value;
    public delegate* unmanaged[Cdecl]<nint, ulong*, byte*, DuckDBState> duckdb_bind_parameter_index;
    public delegate* unmanaged[Cdecl]<nint, ulong, byte, DuckDBState> duckdb_bind_boolean;
    public delegate* unmanaged[Cdecl]<nint, ulong, sbyte, DuckDBState> duckdb_bind_int8;
    public delegate* unmanaged[Cdecl]<nint, ulong, short, DuckDBState> duckdb_bind_int16;
    public delegate* unmanaged[Cdecl]<nint, ulong, int, DuckDBState> duckdb_bind_int32;
    public delegate* unmanaged[Cdecl]<nint, ulong, long, DuckDBState> duckdb_bind_int64;
    public delegate* unmanaged[Cdecl]<nint, ulong, DuckDBHugeInt, DuckDBState> duckdb_bind_hugeint;
    public delegate* unmanaged[Cdecl]<nint, ulong, DuckDBUHugeInt, DuckDBState> duckdb_bind_uhugeint;
    public delegate* unmanaged[Cdecl]<nint, ulong, DuckDBDecimal, DuckDBState> duckdb_bind_decimal;
    public delegate* unmanaged[Cdecl]<nint, ulong, byte, DuckDBState> duckdb_bind_uint8;
    public delegate* unmanaged[Cdecl]<nint, ulong, ushort, DuckDBState> duckdb_bind_uint16;
    public delegate* unmanaged[Cdecl]<nint, ulong, uint, DuckDBState> duckdb_bind_uint32;
    public delegate* unmanaged[Cdecl]<nint, ulong, ulong, DuckDBState> duckdb_bind_uint64;
    public delegate* unmanaged[Cdecl]<nint, ulong, float, DuckDBState> duckdb_bind_float;
    public delegate* unmanaged[Cdecl]<nint, ulong, double, DuckDBState> duckdb_bind_double;
    public delegate* unmanaged[Cdecl]<nint, ulong, DuckDBDate, DuckDBState> duckdb_bind_date;
    public delegate* unmanaged[Cdecl]<nint, ulong, DuckDBTime, DuckDBState> duckdb_bind_time;
    public delegate* unmanaged[Cdecl]<nint, ulong, DuckDBTimestamp, DuckDBState> duckdb_bind_timestamp;
    public delegate* unmanaged[Cdecl]<nint, ulong, DuckDBTimestamp, DuckDBState> duckdb_bind_timestamp_tz;
    public delegate* unmanaged[Cdecl]<nint, ulong, DuckDBInterval, DuckDBState> duckdb_bind_interval;
    public delegate* unmanaged[Cdecl]<nint, ulong, byte*, DuckDBState> duckdb_bind_varchar;
    public delegate* unmanaged[Cdecl]<nint, ulong, byte*, ulong, DuckDBState> duckdb_bind_varchar_length;
    public delegate* unmanaged[Cdecl]<nint, ulong, void*, ulong, DuckDBState> duckdb_bind_blob;
    public delegate* unmanaged[Cdecl]<nint, ulong, DuckDBState> duckdb_bind_null;
    public delegate* unmanaged[Cdecl]<nint, nint*, DuckDBState> duckdb_execute_prepared;

    // --- Extract statements ---
    public delegate* unmanaged[Cdecl]<nint, byte*, nint*, ulong> duckdb_extract_statements;
    public delegate* unmanaged[Cdecl]<nint, nint, ulong, nint*, DuckDBState> duckdb_prepare_extracted_statement;
    public delegate* unmanaged[Cdecl]<nint, byte*> duckdb_extract_statements_error;
    public delegate* unmanaged[Cdecl]<nint*, void> duckdb_destroy_extracted;

    // --- Pending results ---
    public delegate* unmanaged[Cdecl]<nint, nint*, DuckDBState> duckdb_pending_prepared;
    public delegate* unmanaged[Cdecl]<nint*, void> duckdb_destroy_pending;
    public delegate* unmanaged[Cdecl]<nint, byte*> duckdb_pending_error;
    public delegate* unmanaged[Cdecl]<nint, DuckDBPendingState> duckdb_pending_execute_task;
    public delegate* unmanaged[Cdecl]<nint, DuckDBPendingState> duckdb_pending_execute_check_state;
    public delegate* unmanaged[Cdecl]<nint, nint*, DuckDBState> duckdb_execute_pending;
    public delegate* unmanaged[Cdecl]<DuckDBPendingState, byte> duckdb_pending_execution_is_finished;

    // --- Values ---
    public delegate* unmanaged[Cdecl]<nint*, void> duckdb_destroy_value;
    public delegate* unmanaged[Cdecl]<byte*, nint> duckdb_create_varchar;
    public delegate* unmanaged[Cdecl]<byte*, ulong, nint> duckdb_create_varchar_length;
    public delegate* unmanaged[Cdecl]<byte, nint> duckdb_create_bool;
    public delegate* unmanaged[Cdecl]<sbyte, nint> duckdb_create_int8;
    public delegate* unmanaged[Cdecl]<byte, nint> duckdb_create_uint8;
    public delegate* unmanaged[Cdecl]<short, nint> duckdb_create_int16;
    public delegate* unmanaged[Cdecl]<ushort, nint> duckdb_create_uint16;
    public delegate* unmanaged[Cdecl]<int, nint> duckdb_create_int32;
    public delegate* unmanaged[Cdecl]<uint, nint> duckdb_create_uint32;
    public delegate* unmanaged[Cdecl]<ulong, nint> duckdb_create_uint64;
    public delegate* unmanaged[Cdecl]<long, nint> duckdb_create_int64;
    public delegate* unmanaged[Cdecl]<DuckDBHugeInt, nint> duckdb_create_hugeint;
    public delegate* unmanaged[Cdecl]<DuckDBUHugeInt, nint> duckdb_create_uhugeint;
    public delegate* unmanaged[Cdecl]<float, nint> duckdb_create_float;
    public delegate* unmanaged[Cdecl]<double, nint> duckdb_create_double;
    public delegate* unmanaged[Cdecl]<DuckDBDate, nint> duckdb_create_date;
    public delegate* unmanaged[Cdecl]<DuckDBTime, nint> duckdb_create_time;
    public delegate* unmanaged[Cdecl]<DuckDBTimeTz, nint> duckdb_create_time_tz_value;
    public delegate* unmanaged[Cdecl]<DuckDBTimestamp, nint> duckdb_create_timestamp;
    public delegate* unmanaged[Cdecl]<DuckDBInterval, nint> duckdb_create_interval;
    public delegate* unmanaged[Cdecl]<byte*, ulong, nint> duckdb_create_blob;
    public delegate* unmanaged[Cdecl]<DuckDBVarInt, nint> duckdb_create_varint;
    public delegate* unmanaged[Cdecl]<DuckDBDecimal, nint> duckdb_create_decimal;
    public delegate* unmanaged[Cdecl]<DuckDBBit, nint> duckdb_create_bit;
    public delegate* unmanaged[Cdecl]<DuckDBUHugeInt, nint> duckdb_create_uuid;

    // --- Get values ---
    public delegate* unmanaged[Cdecl]<nint, byte> duckdb_get_bool;
    public delegate* unmanaged[Cdecl]<nint, sbyte> duckdb_get_int8;
    public delegate* unmanaged[Cdecl]<nint, byte> duckdb_get_uint8;
    public delegate* unmanaged[Cdecl]<nint, short> duckdb_get_int16;
    public delegate* unmanaged[Cdecl]<nint, ushort> duckdb_get_uint16;
    public delegate* unmanaged[Cdecl]<nint, int> duckdb_get_int32;
    public delegate* unmanaged[Cdecl]<nint, uint> duckdb_get_uint32;
    public delegate* unmanaged[Cdecl]<nint, long> duckdb_get_int64;
    public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_get_uint64;
    public delegate* unmanaged[Cdecl]<nint, DuckDBHugeInt> duckdb_get_hugeint;
    public delegate* unmanaged[Cdecl]<nint, DuckDBUHugeInt> duckdb_get_uhugeint;
    public delegate* unmanaged[Cdecl]<nint, float> duckdb_get_float;
    public delegate* unmanaged[Cdecl]<nint, double> duckdb_get_double;
    public delegate* unmanaged[Cdecl]<nint, DuckDBDate> duckdb_get_date;
    public delegate* unmanaged[Cdecl]<nint, DuckDBTime> duckdb_get_time;
    public delegate* unmanaged[Cdecl]<nint, DuckDBTimeTz> duckdb_get_time_tz;
    public delegate* unmanaged[Cdecl]<nint, DuckDBTimestamp> duckdb_get_timestamp;
    public delegate* unmanaged[Cdecl]<nint, DuckDBInterval> duckdb_get_interval;
    public delegate* unmanaged[Cdecl]<nint, nint> duckdb_get_value_type;
    public delegate* unmanaged[Cdecl]<nint, DuckDBBlob> duckdb_get_blob;
    public delegate* unmanaged[Cdecl]<nint, DuckDBVarInt> duckdb_get_varint;
    public delegate* unmanaged[Cdecl]<nint, DuckDBDecimal> duckdb_get_decimal;
    public delegate* unmanaged[Cdecl]<nint, DuckDBBit> duckdb_get_bit;
    public delegate* unmanaged[Cdecl]<nint, DuckDBUHugeInt> duckdb_get_uuid;
    public delegate* unmanaged[Cdecl]<nint, byte*> duckdb_get_varchar;

    // --- Composite values ---
    public delegate* unmanaged[Cdecl]<nint, nint*, nint> duckdb_create_struct_value;
    public delegate* unmanaged[Cdecl]<nint, nint*, ulong, nint> duckdb_create_list_value;
    public delegate* unmanaged[Cdecl]<nint, nint*, ulong, nint> duckdb_create_array_value;
    public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_get_map_size;
    public delegate* unmanaged[Cdecl]<nint, ulong, nint> duckdb_get_map_key;
    public delegate* unmanaged[Cdecl]<nint, ulong, nint> duckdb_get_map_value;
    public delegate* unmanaged[Cdecl]<nint, byte> duckdb_is_null_value;
    public delegate* unmanaged[Cdecl]<nint> duckdb_create_null_value;
    public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_get_list_size;
    public delegate* unmanaged[Cdecl]<nint, ulong, nint> duckdb_get_list_child;
    public delegate* unmanaged[Cdecl]<nint, ulong, nint> duckdb_create_enum_value;
    public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_get_enum_value;
    public delegate* unmanaged[Cdecl]<nint, ulong, nint> duckdb_get_struct_child;

    // --- Logical types ---
    public delegate* unmanaged[Cdecl]<DuckDBType, nint> duckdb_create_logical_type;
    public delegate* unmanaged[Cdecl]<nint, byte*> duckdb_logical_type_get_alias;
    public delegate* unmanaged[Cdecl]<nint, byte*, void> duckdb_logical_type_set_alias;
    public delegate* unmanaged[Cdecl]<nint, nint> duckdb_create_list_type;
    public delegate* unmanaged[Cdecl]<nint, ulong, nint> duckdb_create_array_type;
    public delegate* unmanaged[Cdecl]<nint, nint, nint> duckdb_create_map_type;
    public delegate* unmanaged[Cdecl]<nint*, byte**, ulong, nint> duckdb_create_union_type;
    public delegate* unmanaged[Cdecl]<nint*, byte**, ulong, nint> duckdb_create_struct_type;
    public delegate* unmanaged[Cdecl]<byte**, ulong, nint> duckdb_create_enum_type;
    public delegate* unmanaged[Cdecl]<byte, byte, nint> duckdb_create_decimal_type;
    public delegate* unmanaged[Cdecl]<nint, DuckDBType> duckdb_get_type_id;
    public delegate* unmanaged[Cdecl]<nint, byte> duckdb_decimal_width;
    public delegate* unmanaged[Cdecl]<nint, byte> duckdb_decimal_scale;
    public delegate* unmanaged[Cdecl]<nint, DuckDBType> duckdb_decimal_internal_type;
    public delegate* unmanaged[Cdecl]<nint, DuckDBType> duckdb_enum_internal_type;
    public delegate* unmanaged[Cdecl]<nint, uint> duckdb_enum_dictionary_size;
    public delegate* unmanaged[Cdecl]<nint, ulong, byte*> duckdb_enum_dictionary_value;
    public delegate* unmanaged[Cdecl]<nint, nint> duckdb_list_type_child_type;
    public delegate* unmanaged[Cdecl]<nint, nint> duckdb_array_type_child_type;
    public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_array_type_array_size;
    public delegate* unmanaged[Cdecl]<nint, nint> duckdb_map_type_key_type;
    public delegate* unmanaged[Cdecl]<nint, nint> duckdb_map_type_value_type;
    public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_struct_type_child_count;
    public delegate* unmanaged[Cdecl]<nint, ulong, byte*> duckdb_struct_type_child_name;
    public delegate* unmanaged[Cdecl]<nint, ulong, nint> duckdb_struct_type_child_type;
    public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_union_type_member_count;
    public delegate* unmanaged[Cdecl]<nint, ulong, byte*> duckdb_union_type_member_name;
    public delegate* unmanaged[Cdecl]<nint, ulong, nint> duckdb_union_type_member_type;
    public delegate* unmanaged[Cdecl]<nint*, void> duckdb_destroy_logical_type;
    public delegate* unmanaged[Cdecl]<nint, nint, nint, DuckDBState> duckdb_register_logical_type;

    // --- Data chunks ---
    public delegate* unmanaged[Cdecl]<nint*, ulong, nint> duckdb_create_data_chunk;
    public delegate* unmanaged[Cdecl]<nint*, void> duckdb_destroy_data_chunk;
    public delegate* unmanaged[Cdecl]<nint, void> duckdb_data_chunk_reset;
    public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_data_chunk_get_column_count;
    public delegate* unmanaged[Cdecl]<nint, ulong, nint> duckdb_data_chunk_get_vector;
    public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_data_chunk_get_size;
    public delegate* unmanaged[Cdecl]<nint, ulong, void> duckdb_data_chunk_set_size;

    // --- Vectors ---
    public delegate* unmanaged[Cdecl]<nint, nint> duckdb_vector_get_column_type;
    public delegate* unmanaged[Cdecl]<nint, void*> duckdb_vector_get_data;
    public delegate* unmanaged[Cdecl]<nint, ulong*> duckdb_vector_get_validity;
    public delegate* unmanaged[Cdecl]<nint, void> duckdb_vector_ensure_validity_writable;
    public delegate* unmanaged[Cdecl]<nint, ulong, byte*, void> duckdb_vector_assign_string_element;
    public delegate* unmanaged[Cdecl]<nint, ulong, byte*, ulong, void> duckdb_vector_assign_string_element_len;
    public delegate* unmanaged[Cdecl]<nint, nint> duckdb_list_vector_get_child;
    public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_list_vector_get_size;
    public delegate* unmanaged[Cdecl]<nint, ulong, DuckDBState> duckdb_list_vector_set_size;
    public delegate* unmanaged[Cdecl]<nint, ulong, DuckDBState> duckdb_list_vector_reserve;
    public delegate* unmanaged[Cdecl]<nint, ulong, nint> duckdb_struct_vector_get_child;
    public delegate* unmanaged[Cdecl]<nint, nint> duckdb_array_vector_get_child;

    // --- Validity ---
    public delegate* unmanaged[Cdecl]<ulong*, ulong, byte> duckdb_validity_row_is_valid;
    public delegate* unmanaged[Cdecl]<ulong*, ulong, byte, void> duckdb_validity_set_row_validity;
    public delegate* unmanaged[Cdecl]<ulong*, ulong, void> duckdb_validity_set_row_invalid;
    public delegate* unmanaged[Cdecl]<ulong*, ulong, void> duckdb_validity_set_row_valid;

    // --- Scalar functions ---
    public delegate* unmanaged[Cdecl]<nint> duckdb_create_scalar_function;
    public delegate* unmanaged[Cdecl]<nint*, void> duckdb_destroy_scalar_function;
    public delegate* unmanaged[Cdecl]<nint, byte*, void> duckdb_scalar_function_set_name;
    public delegate* unmanaged[Cdecl]<nint, nint, void> duckdb_scalar_function_set_varargs;
    public delegate* unmanaged[Cdecl]<nint, void> duckdb_scalar_function_set_special_handling;
    public delegate* unmanaged[Cdecl]<nint, void> duckdb_scalar_function_set_volatile;
    public delegate* unmanaged[Cdecl]<nint, nint, void> duckdb_scalar_function_add_parameter;
    public delegate* unmanaged[Cdecl]<nint, nint, void> duckdb_scalar_function_set_return_type;
    public delegate* unmanaged[Cdecl]<nint, void*, delegate* unmanaged[Cdecl]<void*, void>, void> duckdb_scalar_function_set_extra_info;
    public delegate* unmanaged[Cdecl]<nint, delegate* unmanaged[Cdecl]<nint, nint, nint, void>, void> duckdb_scalar_function_set_function;
    public delegate* unmanaged[Cdecl]<nint, nint, DuckDBState> duckdb_register_scalar_function;
    public delegate* unmanaged[Cdecl]<nint, void*> duckdb_scalar_function_get_extra_info;
    public delegate* unmanaged[Cdecl]<nint, byte*, void> duckdb_scalar_function_set_error;

    // --- Scalar function sets ---
    public delegate* unmanaged[Cdecl]<byte*, nint> duckdb_create_scalar_function_set;
    public delegate* unmanaged[Cdecl]<nint*, void> duckdb_destroy_scalar_function_set;
    public delegate* unmanaged[Cdecl]<nint, nint, DuckDBState> duckdb_add_scalar_function_to_set;
    public delegate* unmanaged[Cdecl]<nint, nint, DuckDBState> duckdb_register_scalar_function_set;

    // --- Aggregate functions ---
    public delegate* unmanaged[Cdecl]<nint> duckdb_create_aggregate_function;
    public delegate* unmanaged[Cdecl]<nint*, void> duckdb_destroy_aggregate_function;
    public delegate* unmanaged[Cdecl]<nint, byte*, void> duckdb_aggregate_function_set_name;
    public delegate* unmanaged[Cdecl]<nint, nint, void> duckdb_aggregate_function_add_parameter;
    public delegate* unmanaged[Cdecl]<nint, nint, void> duckdb_aggregate_function_set_return_type;
    public delegate* unmanaged[Cdecl]<nint, delegate* unmanaged[Cdecl]<nint, ulong>, delegate* unmanaged[Cdecl]<nint, void*, void>, delegate* unmanaged[Cdecl]<nint, nint, void*, ulong, void>, delegate* unmanaged[Cdecl]<nint, void*, void*, void>, delegate* unmanaged[Cdecl]<nint, void*, nint, void>, void> duckdb_aggregate_function_set_functions;
    public delegate* unmanaged[Cdecl]<nint, delegate* unmanaged[Cdecl]<void**, ulong, void>, void> duckdb_aggregate_function_set_destructor;
    public delegate* unmanaged[Cdecl]<nint, nint, DuckDBState> duckdb_register_aggregate_function;
    public delegate* unmanaged[Cdecl]<nint, void> duckdb_aggregate_function_set_special_handling;
    public delegate* unmanaged[Cdecl]<nint, void*, delegate* unmanaged[Cdecl]<void*, void>, void> duckdb_aggregate_function_set_extra_info;
    public delegate* unmanaged[Cdecl]<nint, void*> duckdb_aggregate_function_get_extra_info;
    public delegate* unmanaged[Cdecl]<nint, byte*, void> duckdb_aggregate_function_set_error;

    // --- Aggregate function sets ---
    public delegate* unmanaged[Cdecl]<byte*, nint> duckdb_create_aggregate_function_set;
    public delegate* unmanaged[Cdecl]<nint*, void> duckdb_destroy_aggregate_function_set;
    public delegate* unmanaged[Cdecl]<nint, nint, DuckDBState> duckdb_add_aggregate_function_to_set;
    public delegate* unmanaged[Cdecl]<nint, nint, DuckDBState> duckdb_register_aggregate_function_set;

    // --- Table functions ---
    public delegate* unmanaged[Cdecl]<nint> duckdb_create_table_function;
    public delegate* unmanaged[Cdecl]<nint*, void> duckdb_destroy_table_function;
    public delegate* unmanaged[Cdecl]<nint, byte*, void> duckdb_table_function_set_name;
    public delegate* unmanaged[Cdecl]<nint, nint, void> duckdb_table_function_add_parameter;
    public delegate* unmanaged[Cdecl]<nint, byte*, nint, void> duckdb_table_function_add_named_parameter;
    public delegate* unmanaged[Cdecl]<nint, void*, delegate* unmanaged[Cdecl]<void*, void>, void> duckdb_table_function_set_extra_info;
    public delegate* unmanaged[Cdecl]<nint, delegate* unmanaged[Cdecl]<nint, void>, void> duckdb_table_function_set_bind;
    public delegate* unmanaged[Cdecl]<nint, delegate* unmanaged[Cdecl]<nint, void>, void> duckdb_table_function_set_init;
    public delegate* unmanaged[Cdecl]<nint, delegate* unmanaged[Cdecl]<nint, void>, void> duckdb_table_function_set_local_init;
    public delegate* unmanaged[Cdecl]<nint, delegate* unmanaged[Cdecl]<nint, nint, void>, void> duckdb_table_function_set_function;
    public delegate* unmanaged[Cdecl]<nint, byte, void> duckdb_table_function_supports_projection_pushdown;
    public delegate* unmanaged[Cdecl]<nint, nint, DuckDBState> duckdb_register_table_function;

    // --- Bind info ---
    public delegate* unmanaged[Cdecl]<nint, void*> duckdb_bind_get_extra_info;
    public delegate* unmanaged[Cdecl]<nint, byte*, nint, void> duckdb_bind_add_result_column;
    public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_bind_get_parameter_count;
    public delegate* unmanaged[Cdecl]<nint, ulong, nint> duckdb_bind_get_parameter;
    public delegate* unmanaged[Cdecl]<nint, byte*, nint> duckdb_bind_get_named_parameter;
    public delegate* unmanaged[Cdecl]<nint, void*, delegate* unmanaged[Cdecl]<void*, void>, void> duckdb_bind_set_bind_data;
    public delegate* unmanaged[Cdecl]<nint, ulong, byte, void> duckdb_bind_set_cardinality;
    public delegate* unmanaged[Cdecl]<nint, byte*, void> duckdb_bind_set_error;

    // --- Init info ---
    public delegate* unmanaged[Cdecl]<nint, void*> duckdb_init_get_extra_info;
    public delegate* unmanaged[Cdecl]<nint, void*> duckdb_init_get_bind_data;
    public delegate* unmanaged[Cdecl]<nint, void*, delegate* unmanaged[Cdecl]<void*, void>, void> duckdb_init_set_init_data;
    public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_init_get_column_count;
    public delegate* unmanaged[Cdecl]<nint, ulong, ulong> duckdb_init_get_column_index;
    public delegate* unmanaged[Cdecl]<nint, ulong, void> duckdb_init_set_max_threads;
    public delegate* unmanaged[Cdecl]<nint, byte*, void> duckdb_init_set_error;

    // --- Function info ---
    public delegate* unmanaged[Cdecl]<nint, void*> duckdb_function_get_extra_info;
    public delegate* unmanaged[Cdecl]<nint, void*> duckdb_function_get_bind_data;
    public delegate* unmanaged[Cdecl]<nint, void*> duckdb_function_get_init_data;
    public delegate* unmanaged[Cdecl]<nint, void*> duckdb_function_get_local_init_data;
    public delegate* unmanaged[Cdecl]<nint, byte*, void> duckdb_function_set_error;

    // --- Replacement scans ---
    public delegate* unmanaged[Cdecl]<nint, delegate* unmanaged[Cdecl]<nint, byte*, void*, void>, void*, delegate* unmanaged[Cdecl]<void*, void>, void> duckdb_add_replacement_scan;
    public delegate* unmanaged[Cdecl]<nint, byte*, void> duckdb_replacement_scan_set_function_name;
    public delegate* unmanaged[Cdecl]<nint, nint, void> duckdb_replacement_scan_add_parameter;
    public delegate* unmanaged[Cdecl]<nint, byte*, void> duckdb_replacement_scan_set_error;

    // --- Profiling ---
    public delegate* unmanaged[Cdecl]<nint, nint> duckdb_profiling_info_get_metrics;
    public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_profiling_info_get_child_count;
    public delegate* unmanaged[Cdecl]<nint, ulong, nint> duckdb_profiling_info_get_child;

    // --- Appender ---
    public delegate* unmanaged[Cdecl]<nint, byte*, byte*, nint*, DuckDBState> duckdb_appender_create;
    public delegate* unmanaged[Cdecl]<nint, byte*, byte*, byte*, nint*, DuckDBState> duckdb_appender_create_ext;
    public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_appender_column_count;
    public delegate* unmanaged[Cdecl]<nint, ulong, nint> duckdb_appender_column_type;
    public delegate* unmanaged[Cdecl]<nint, byte*> duckdb_appender_error;
    public delegate* unmanaged[Cdecl]<nint, DuckDBState> duckdb_appender_flush;
    public delegate* unmanaged[Cdecl]<nint, DuckDBState> duckdb_appender_close;
    public delegate* unmanaged[Cdecl]<nint*, DuckDBState> duckdb_appender_destroy;
    public delegate* unmanaged[Cdecl]<nint, byte*, DuckDBState> duckdb_appender_add_column;
    public delegate* unmanaged[Cdecl]<nint, DuckDBState> duckdb_appender_clear_columns;
    public delegate* unmanaged[Cdecl]<nint, nint, DuckDBState> duckdb_append_data_chunk;

    // --- Table description ---
    public delegate* unmanaged[Cdecl]<nint, byte*, byte*, nint*, DuckDBState> duckdb_table_description_create;
    public delegate* unmanaged[Cdecl]<nint, byte*, byte*, byte*, nint*, DuckDBState> duckdb_table_description_create_ext;
    public delegate* unmanaged[Cdecl]<nint*, void> duckdb_table_description_destroy;
    public delegate* unmanaged[Cdecl]<nint, byte*> duckdb_table_description_error;
    public delegate* unmanaged[Cdecl]<nint, ulong, byte*, DuckDBState> duckdb_column_has_default;
    public delegate* unmanaged[Cdecl]<nint, ulong, byte*> duckdb_table_description_get_column_name;

    // --- Tasks ---
    public delegate* unmanaged[Cdecl]<nint, ulong, void> duckdb_execute_tasks;
    public delegate* unmanaged[Cdecl]<nint, nint> duckdb_create_task_state;
    public delegate* unmanaged[Cdecl]<nint, void> duckdb_execute_tasks_state;
    public delegate* unmanaged[Cdecl]<nint, ulong, ulong> duckdb_execute_n_tasks_state;
    public delegate* unmanaged[Cdecl]<nint, void> duckdb_finish_execution;
    public delegate* unmanaged[Cdecl]<nint, byte> duckdb_task_state_is_finished;
    public delegate* unmanaged[Cdecl]<nint, void> duckdb_destroy_task_state;
    public delegate* unmanaged[Cdecl]<nint, byte> duckdb_execution_is_finished;

    // --- Fetch ---
    public delegate* unmanaged[Cdecl]<nint, nint> duckdb_fetch_chunk;

    // --- Cast functions ---
    public delegate* unmanaged[Cdecl]<nint> duckdb_create_cast_function;
    public delegate* unmanaged[Cdecl]<nint, nint, void> duckdb_cast_function_set_source_type;
    public delegate* unmanaged[Cdecl]<nint, nint, void> duckdb_cast_function_set_target_type;
    public delegate* unmanaged[Cdecl]<nint, long, void> duckdb_cast_function_set_implicit_cast_cost;
    public delegate* unmanaged[Cdecl]<nint, delegate* unmanaged[Cdecl]<nint, ulong, nint, nint, byte>, void> duckdb_cast_function_set_function;
    public delegate* unmanaged[Cdecl]<nint, void*, delegate* unmanaged[Cdecl]<void*, void>, void> duckdb_cast_function_set_extra_info;
    public delegate* unmanaged[Cdecl]<nint, void*> duckdb_cast_function_get_extra_info;
    public delegate* unmanaged[Cdecl]<nint, DuckDBCastMode> duckdb_cast_function_get_cast_mode;
    public delegate* unmanaged[Cdecl]<nint, byte*, void> duckdb_cast_function_set_error;
    public delegate* unmanaged[Cdecl]<nint, byte*, ulong, nint, void> duckdb_cast_function_set_row_error;
    public delegate* unmanaged[Cdecl]<nint, nint, DuckDBState> duckdb_register_cast_function;
    public delegate* unmanaged[Cdecl]<nint*, void> duckdb_destroy_cast_function;

    // --- Timestamp variants ---
    public delegate* unmanaged[Cdecl]<DuckDBTimestampS, byte> duckdb_is_finite_timestamp_s;
    public delegate* unmanaged[Cdecl]<DuckDBTimestampMs, byte> duckdb_is_finite_timestamp_ms;
    public delegate* unmanaged[Cdecl]<DuckDBTimestampNs, byte> duckdb_is_finite_timestamp_ns;
    public delegate* unmanaged[Cdecl]<DuckDBTimestamp, nint> duckdb_create_timestamp_tz;
    public delegate* unmanaged[Cdecl]<DuckDBTimestampS, nint> duckdb_create_timestamp_s;
    public delegate* unmanaged[Cdecl]<DuckDBTimestampMs, nint> duckdb_create_timestamp_ms;
    public delegate* unmanaged[Cdecl]<DuckDBTimestampNs, nint> duckdb_create_timestamp_ns;
    public delegate* unmanaged[Cdecl]<nint, DuckDBTimestamp> duckdb_get_timestamp_tz;
    public delegate* unmanaged[Cdecl]<nint, DuckDBTimestampS> duckdb_get_timestamp_s;
    public delegate* unmanaged[Cdecl]<nint, DuckDBTimestampMs> duckdb_get_timestamp_ms;
    public delegate* unmanaged[Cdecl]<nint, DuckDBTimestampNs> duckdb_get_timestamp_ns;

    // --- Appender value/row ---
    public delegate* unmanaged[Cdecl]<nint, nint, DuckDBState> duckdb_append_value;
    public delegate* unmanaged[Cdecl]<nint, nint> duckdb_get_profiling_info;
    public delegate* unmanaged[Cdecl]<nint, byte*, nint> duckdb_profiling_info_get_value;
    public delegate* unmanaged[Cdecl]<nint, DuckDBState> duckdb_appender_begin_row;
    public delegate* unmanaged[Cdecl]<nint, DuckDBState> duckdb_appender_end_row;
    public delegate* unmanaged[Cdecl]<nint, DuckDBState> duckdb_append_default;
    public delegate* unmanaged[Cdecl]<nint, byte, DuckDBState> duckdb_append_bool;
    public delegate* unmanaged[Cdecl]<nint, sbyte, DuckDBState> duckdb_append_int8;
    public delegate* unmanaged[Cdecl]<nint, short, DuckDBState> duckdb_append_int16;
    public delegate* unmanaged[Cdecl]<nint, int, DuckDBState> duckdb_append_int32;
    public delegate* unmanaged[Cdecl]<nint, long, DuckDBState> duckdb_append_int64;
    public delegate* unmanaged[Cdecl]<nint, DuckDBHugeInt, DuckDBState> duckdb_append_hugeint;
    public delegate* unmanaged[Cdecl]<nint, byte, DuckDBState> duckdb_append_uint8;
    public delegate* unmanaged[Cdecl]<nint, ushort, DuckDBState> duckdb_append_uint16;
    public delegate* unmanaged[Cdecl]<nint, uint, DuckDBState> duckdb_append_uint32;
    public delegate* unmanaged[Cdecl]<nint, ulong, DuckDBState> duckdb_append_uint64;
    public delegate* unmanaged[Cdecl]<nint, DuckDBUHugeInt, DuckDBState> duckdb_append_uhugeint;
    public delegate* unmanaged[Cdecl]<nint, float, DuckDBState> duckdb_append_float;
    public delegate* unmanaged[Cdecl]<nint, double, DuckDBState> duckdb_append_double;
    public delegate* unmanaged[Cdecl]<nint, DuckDBDate, DuckDBState> duckdb_append_date;
    public delegate* unmanaged[Cdecl]<nint, DuckDBTime, DuckDBState> duckdb_append_time;
    public delegate* unmanaged[Cdecl]<nint, DuckDBTimestamp, DuckDBState> duckdb_append_timestamp;
    public delegate* unmanaged[Cdecl]<nint, DuckDBInterval, DuckDBState> duckdb_append_interval;
    public delegate* unmanaged[Cdecl]<nint, byte*, DuckDBState> duckdb_append_varchar;
    public delegate* unmanaged[Cdecl]<nint, byte*, ulong, DuckDBState> duckdb_append_varchar_length;
    public delegate* unmanaged[Cdecl]<nint, void*, ulong, DuckDBState> duckdb_append_blob;
    public delegate* unmanaged[Cdecl]<nint, DuckDBState> duckdb_append_null;

    // === END OF STABLE API ===

#if DUCKDB_EXTENSION_API_VERSION_UNSTABLE
      // --- Deprecated functions ---
      public delegate* unmanaged[Cdecl]<nint*, ulong> duckdb_row_count;
      public delegate* unmanaged[Cdecl]<nint*, ulong, void*> duckdb_column_data;
      public delegate* unmanaged[Cdecl]<nint*, ulong, byte*> duckdb_nullmask_data;
      public delegate* unmanaged[Cdecl]<nint, ulong, nint> duckdb_result_get_chunk;
      public delegate* unmanaged[Cdecl]<nint, byte> duckdb_result_is_streaming;
      public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_result_chunk_count;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, byte> duckdb_value_boolean;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, sbyte> duckdb_value_int8;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, short> duckdb_value_int16;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, int> duckdb_value_int32;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, long> duckdb_value_int64;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, DuckDBHugeInt> duckdb_value_hugeint;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, DuckDBUHugeInt> duckdb_value_uhugeint;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, DuckDBDecimal> duckdb_value_decimal;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, byte> duckdb_value_uint8;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, ushort> duckdb_value_uint16;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, uint> duckdb_value_uint32;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, ulong> duckdb_value_uint64;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, float> duckdb_value_float;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, double> duckdb_value_double;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, DuckDBDate> duckdb_value_date;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, DuckDBTime> duckdb_value_time;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, DuckDBTimestamp> duckdb_value_timestamp;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, DuckDBInterval> duckdb_value_interval;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, byte*> duckdb_value_varchar;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, DuckDBStringT> duckdb_value_string;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, byte*> duckdb_value_varchar_internal;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, DuckDBStringT> duckdb_value_string_internal;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, DuckDBBlob> duckdb_value_blob;
      public delegate* unmanaged[Cdecl]<nint*, ulong, ulong, byte> duckdb_value_is_null;
      public delegate* unmanaged[Cdecl]<nint, nint*, DuckDBState> duckdb_execute_prepared_streaming;
      public delegate* unmanaged[Cdecl]<nint, nint*, DuckDBState> duckdb_pending_prepared_streaming;

      // --- Arrow functions ---
      public delegate* unmanaged[Cdecl]<nint, byte*, nint*, DuckDBState> duckdb_query_arrow;
      public delegate* unmanaged[Cdecl]<nint, nint*, DuckDBState> duckdb_query_arrow_schema;
      public delegate* unmanaged[Cdecl]<nint, nint*, DuckDBState> duckdb_prepared_arrow_schema;
      public delegate* unmanaged[Cdecl]<nint, nint, nint*, void> duckdb_result_arrow_array;
      public delegate* unmanaged[Cdecl]<nint, nint*, DuckDBState> duckdb_query_arrow_array;
      public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_arrow_column_count;
      public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_arrow_row_count;
      public delegate* unmanaged[Cdecl]<nint, ulong> duckdb_arrow_rows_changed;
      public delegate* unmanaged[Cdecl]<nint, byte*> duckdb_query_arrow_error;
      public delegate* unmanaged[Cdecl]<nint*, void> duckdb_destroy_arrow;
      public delegate* unmanaged[Cdecl]<nint*, void> duckdb_destroy_arrow_stream;
      public delegate* unmanaged[Cdecl]<nint, nint*, DuckDBState> duckdb_execute_prepared_arrow;
      public delegate* unmanaged[Cdecl]<nint, byte*, nint, DuckDBState> duckdb_arrow_scan;
      public delegate* unmanaged[Cdecl]<nint, byte*, nint, nint, nint*, DuckDBState> duckdb_arrow_array_scan;
      public delegate* unmanaged[Cdecl]<nint, nint> duckdb_stream_fetch_chunk;

      // --- Instance cache ---
      public delegate* unmanaged[Cdecl]<nint> duckdb_create_instance_cache;
      public delegate* unmanaged[Cdecl]<nint, byte*, nint*, nint, byte**, DuckDBState> duckdb_get_or_create_from_cache;
      public delegate* unmanaged[Cdecl]<nint*, void> duckdb_destroy_instance_cache;

      // --- New append functions ---
      public delegate* unmanaged[Cdecl]<nint, nint, ulong, ulong, DuckDBState> duckdb_append_default_to_chunk;
#endif
}