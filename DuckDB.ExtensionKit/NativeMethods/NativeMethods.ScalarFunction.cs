using DuckDB.ExtensionKit.Native;

namespace DuckDB.ExtensionKit.NativeMethods;

public static partial class NativeMethods
{
    internal static unsafe class ScalarFunction
    {
        internal static IntPtr DuckDBCreateScalarFunction() =>
            Api.duckdb_create_scalar_function();

        internal static void DuckDBDestroyScalarFunction(IntPtr* scalarFunction) =>
            Api.duckdb_destroy_scalar_function(scalarFunction);

        internal static void DuckDBScalarFunctionSetName(IntPtr scalarFunction, byte* name) =>
            Api.duckdb_scalar_function_set_name(scalarFunction, name);

        internal static void DuckDBScalarFunctionSetVarargs(IntPtr scalarFunction, DuckDBLogicalType type) =>
            Api.duckdb_scalar_function_set_varargs(scalarFunction, type.DangerousGetHandle());

        internal static void DuckDBScalarFunctionSetVolatile(IntPtr scalarFunction) =>
            Api.duckdb_scalar_function_set_volatile(scalarFunction);

        internal static void DuckDBScalarFunctionAddParameter(IntPtr scalarFunction, DuckDBLogicalType type) =>
            Api.duckdb_scalar_function_add_parameter(scalarFunction, type.DangerousGetHandle());

        internal static void DuckDBScalarFunctionSetReturnType(IntPtr scalarFunction, DuckDBLogicalType type) =>
            Api.duckdb_scalar_function_set_return_type(scalarFunction, type.DangerousGetHandle());

        internal static void DuckDBScalarFunctionSetExtraInfo(IntPtr scalarFunction, IntPtr extraInfo, delegate* unmanaged[Cdecl]<void*, void> destroy) =>
            Api.duckdb_scalar_function_set_extra_info(scalarFunction, extraInfo.ToPointer(), destroy);

        internal static void DuckDBScalarFunctionSetFunction(IntPtr scalarFunction, delegate* unmanaged[Cdecl]<IntPtr, IntPtr, IntPtr, void> callback) =>
            Api.duckdb_scalar_function_set_function(scalarFunction, callback);

        internal static DuckDBState DuckDBRegisterScalarFunction(IntPtr connection, IntPtr scalarFunction) =>
            Api.duckdb_register_scalar_function(connection, scalarFunction);

        internal static void* DuckDBScalarFunctionGetExtraInfo(IntPtr scalarFunction) =>
            Api.duckdb_scalar_function_get_extra_info(scalarFunction);
        
        internal static void DuckDBScalarFunctionSetError(IntPtr info, byte* error) =>
            Api.duckdb_scalar_function_set_error(info, error);
    }
}
