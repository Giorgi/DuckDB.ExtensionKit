using DuckDB.ExtensionKit.Native;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

public static partial class NativeMethods
{
    // The signatures of DuckDB.NET.Bindings (NativeMethods.TableFunction.cs), which the registration code linked
    // from DuckDB.NET calls.
    internal static unsafe class TableFunction
    {
        internal static IntPtr DuckDBCreateTableFunction() => Api.duckdb_create_table_function();

        internal static void DuckDBDestroyTableFunction(ref IntPtr tableFunction)
        {
            fixed (IntPtr* pointer = &tableFunction)
            {
                Api.duckdb_destroy_table_function(pointer);
            }
        }

        internal static void DuckDBTableFunctionSetName(IntPtr tableFunction, string name)
        {
            fixed (byte* pointer = Helpers.ToUtf8(name))
            {
                Api.duckdb_table_function_set_name(tableFunction, pointer);
            }
        }

        internal static void DuckDBTableFunctionAddParameter(IntPtr tableFunction, DuckDBLogicalType type) =>
            Api.duckdb_table_function_add_parameter(tableFunction, type.DangerousGetHandle());

        internal static void DuckDBTableFunctionAddNamedParameter(IntPtr tableFunction, string name, DuckDBLogicalType type)
        {
            fixed (byte* pointer = Helpers.ToUtf8(name))
            {
                Api.duckdb_table_function_add_named_parameter(tableFunction, pointer, type.DangerousGetHandle());
            }
        }

        internal static void DuckDBTableFunctionSetExtraInfo(IntPtr tableFunction, IntPtr extraInfo, delegate* unmanaged[Cdecl]<IntPtr, void> destroy) =>
            Api.duckdb_table_function_set_extra_info(tableFunction, extraInfo.ToPointer(), (delegate* unmanaged[Cdecl]<void*, void>)destroy);

        internal static void DuckDBTableFunctionSetBind(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, void> bind) =>
            Api.duckdb_table_function_set_bind(tableFunction, bind);

        internal static void DuckDBTableFunctionSetInit(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, void> init) =>
            Api.duckdb_table_function_set_init(tableFunction, init);

        internal static void DuckDBTableFunctionSetFunction(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, IntPtr, void> callback) =>
            Api.duckdb_table_function_set_function(tableFunction, callback);

        internal static void DuckDBTableFunctionSupportsProjectionPushdown(IntPtr tableFunction, bool pushdown) =>
            Api.duckdb_table_function_supports_projection_pushdown(tableFunction, (byte)(pushdown ? 1 : 0));

        internal static DuckDBState DuckDBRegisterTableFunction(DuckDBNativeConnection connection, IntPtr tableFunction) =>
            Api.duckdb_register_table_function(connection.Handle, tableFunction);

        // Bind
        internal static IntPtr DuckDBBindGetExtraInfo(IntPtr info) => new(Api.duckdb_bind_get_extra_info(info));

        internal static void DuckDBBindAddResultColumn(IntPtr info, string name, DuckDBLogicalType type)
        {
            fixed (byte* pointer = Helpers.ToUtf8(name))
            {
                Api.duckdb_bind_add_result_column(info, pointer, type.DangerousGetHandle());
            }
        }

        internal static ulong DuckDBBindGetParameterCount(IntPtr info) => Api.duckdb_bind_get_parameter_count(info);

        // The caller owns the value: DuckDBValue destroys it.
        internal static DuckDBValue DuckDBBindGetParameter(IntPtr info, ulong index) => ToValue(Api.duckdb_bind_get_parameter(info, index));

        // A value that is invalid (IsInvalid) when the named parameter was left out in SQL.
        internal static DuckDBValue DuckDBBindGetNamedParameter(IntPtr info, string name)
        {
            fixed (byte* pointer = Helpers.ToUtf8(name))
            {
                return ToValue(Api.duckdb_bind_get_named_parameter(info, pointer));
            }
        }

        internal static void DuckDBBindSetBindData(IntPtr info, IntPtr bindData, delegate* unmanaged[Cdecl]<IntPtr, void> destroy) =>
            Api.duckdb_bind_set_bind_data(info, bindData.ToPointer(), (delegate* unmanaged[Cdecl]<void*, void>)destroy);

        internal static void DuckDBBindSetCardinality(IntPtr info, ulong cardinality, bool isExact) =>
            Api.duckdb_bind_set_cardinality(info, cardinality, (byte)(isExact ? 1 : 0));

        internal static void DuckDBBindSetError(IntPtr info, string error)
        {
            fixed (byte* pointer = Helpers.ToUtf8(error))
            {
                Api.duckdb_bind_set_error(info, pointer);
            }
        }

        // Init
        internal static IntPtr DuckDBInitGetExtraInfo(IntPtr info) => new(Api.duckdb_init_get_extra_info(info));

        internal static IntPtr DuckDBInitGetBindData(IntPtr info) => new(Api.duckdb_init_get_bind_data(info));

        internal static void DuckDBInitSetInitData(IntPtr info, IntPtr initData, delegate* unmanaged[Cdecl]<IntPtr, void> destroy) =>
            Api.duckdb_init_set_init_data(info, initData.ToPointer(), (delegate* unmanaged[Cdecl]<void*, void>)destroy);

        internal static ulong DuckDBInitGetColumnCount(IntPtr info) => Api.duckdb_init_get_column_count(info);

        internal static ulong DuckDBInitGetColumnIndex(IntPtr info, ulong columnIndex) => Api.duckdb_init_get_column_index(info, columnIndex);

        internal static void DuckDBInitSetError(IntPtr info, string error)
        {
            fixed (byte* pointer = Helpers.ToUtf8(error))
            {
                Api.duckdb_init_set_error(info, pointer);
            }
        }

        // Function
        internal static IntPtr DuckDBFunctionGetBindData(IntPtr info) => new(Api.duckdb_function_get_bind_data(info));

        internal static IntPtr DuckDBFunctionGetInitData(IntPtr info) => new(Api.duckdb_function_get_init_data(info));

        internal static void DuckDBFunctionSetError(IntPtr info, string error)
        {
            fixed (byte* pointer = Helpers.ToUtf8(error))
            {
                Api.duckdb_function_set_error(info, pointer);
            }
        }

        // DuckDB.NET's DuckDBValue gets its handle from the marshalling of LibraryImport; here it is set the same way.
        private static DuckDBValue ToValue(IntPtr handle)
        {
            var value = new DuckDBValue();
            Marshal.InitHandle(value, handle);
            return value;
        }
    }
}
