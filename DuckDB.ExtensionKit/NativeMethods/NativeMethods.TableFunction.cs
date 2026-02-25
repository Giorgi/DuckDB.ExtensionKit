using DuckDB.ExtensionKit.Native;

namespace DuckDB.ExtensionKit.NativeMethods;

public static partial class NativeMethods
{
    internal static unsafe class TableFunction
    {
        internal static IntPtr DuckDBCreateTableFunction() => Api.duckdb_create_table_function();

        internal static void DuckDBDestroyTableFunction(IntPtr tableFunction) => Api.duckdb_destroy_table_function(&tableFunction);

        internal static void DuckDBTableFunctionSetName(IntPtr tableFunction, byte* name) => Api.duckdb_table_function_set_name(tableFunction, name);

        internal static void DuckDBTableFunctionAddParameter(IntPtr tableFunction, DuckDBLogicalType type) =>
            Api.duckdb_table_function_add_parameter(tableFunction, type.DangerousGetHandle());

        internal static void DuckDBTableFunctionAddNamedParameter(IntPtr tableFunction, byte* name, DuckDBLogicalType type) =>
            Api.duckdb_table_function_add_named_parameter(tableFunction, name, type.DangerousGetHandle());

        internal static void DuckDBTableFunctionSetExtraInfo(IntPtr tableFunction, IntPtr extraInfo, delegate* unmanaged[Cdecl]<void*, void> destroy) =>
            Api.duckdb_table_function_set_extra_info(tableFunction, extraInfo.ToPointer(), destroy);

        internal static void DuckDBTableFunctionSetBind(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, void> bind) =>
            Api.duckdb_table_function_set_bind(tableFunction, bind);

        internal static void DuckDBTableFunctionSetInit(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, void> init) =>
            Api.duckdb_table_function_set_init(tableFunction, init);

        internal static void DuckDBTableFunctionSetFunction(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, IntPtr, void> function) =>
            Api.duckdb_table_function_set_function(tableFunction, function);

        internal static DuckDBState DuckDBRegisterTableFunction(IntPtr connection, IntPtr tableFunction) =>
            Api.duckdb_register_table_function(connection, tableFunction);

        // Bind methods
        internal static void* DuckDBBindGetExtraInfo(IntPtr info) =>
            Api.duckdb_bind_get_extra_info(info);

        internal static ulong DuckDBBindGetParameterCount(IntPtr info) =>
            Api.duckdb_bind_get_parameter_count(info);

        internal static IntPtr DuckDBBindGetParameter(IntPtr info, ulong index) =>
            Api.duckdb_bind_get_parameter(info, index);

        internal static IntPtr DuckDBBindGetNamedParameter(IntPtr info, byte* name) =>
            Api.duckdb_bind_get_named_parameter(info, name);

        internal static void DuckDBBindAddResultColumn(IntPtr info, byte* name, DuckDBLogicalType type) =>
            Api.duckdb_bind_add_result_column(info, name, type.DangerousGetHandle());

        internal static void DuckDBBindSetBindData(IntPtr info, IntPtr bindData, delegate* unmanaged[Cdecl]<void*, void> destroy) =>
            Api.duckdb_bind_set_bind_data(info, bindData.ToPointer(), destroy);

        internal static void DuckDBBindSetError(IntPtr info, byte* error) =>
            Api.duckdb_bind_set_error(info, error);

        // Function methods
        internal static void* DuckDBFunctionGetBindData(IntPtr info) =>
            Api.duckdb_function_get_bind_data(info);

        internal static void* DuckDBFunctionGetExtraInfo(IntPtr info) =>
            Api.duckdb_function_get_extra_info(info);

        internal static void DuckDBFunctionSetError(IntPtr info, byte* error) =>
            Api.duckdb_function_set_error(info, error);
    }
}
