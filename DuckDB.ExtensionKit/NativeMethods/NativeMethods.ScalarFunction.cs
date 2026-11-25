using DuckDB.ExtensionKit.Native;

namespace DuckDB.NET.Native;

public static partial class NativeMethods
{
    // The signatures of DuckDB.NET.Bindings (NativeMethods.ScalarFunction.cs), which the registration code linked
    // from DuckDB.NET calls.
    internal static unsafe class ScalarFunction
    {
        internal static IntPtr DuckDBCreateScalarFunction() => Api.duckdb_create_scalar_function();

        internal static void DuckDBDestroyScalarFunction(ref IntPtr scalarFunction)
        {
            fixed (IntPtr* pointer = &scalarFunction)
            {
                Api.duckdb_destroy_scalar_function(pointer);
            }
        }

        internal static void DuckDBScalarFunctionSetName(IntPtr scalarFunction, string name)
        {
            fixed (byte* pointer = Helpers.ToUtf8(name))
            {
                Api.duckdb_scalar_function_set_name(scalarFunction, pointer);
            }
        }

        internal static void DuckDBScalarFunctionSetVarargs(IntPtr scalarFunction, DuckDBLogicalType type) =>
            Api.duckdb_scalar_function_set_varargs(scalarFunction, type.DangerousGetHandle());

        internal static void DuckDBScalarFunctionSetSpecialHandling(IntPtr scalarFunction) =>
            Api.duckdb_scalar_function_set_special_handling(scalarFunction);

        internal static void DuckDBScalarFunctionSetVolatile(IntPtr scalarFunction) =>
            Api.duckdb_scalar_function_set_volatile(scalarFunction);

        internal static void DuckDBScalarFunctionAddParameter(IntPtr scalarFunction, DuckDBLogicalType type) =>
            Api.duckdb_scalar_function_add_parameter(scalarFunction, type.DangerousGetHandle());

        internal static void DuckDBScalarFunctionSetReturnType(IntPtr scalarFunction, DuckDBLogicalType type) =>
            Api.duckdb_scalar_function_set_return_type(scalarFunction, type.DangerousGetHandle());

        internal static void DuckDBScalarFunctionSetExtraInfo(IntPtr scalarFunction, IntPtr extraInfo, delegate* unmanaged[Cdecl]<IntPtr, void> destroy) =>
            Api.duckdb_scalar_function_set_extra_info(scalarFunction, extraInfo.ToPointer(), (delegate* unmanaged[Cdecl]<void*, void>)destroy);

        internal static void DuckDBScalarFunctionSetFunction(IntPtr scalarFunction, delegate* unmanaged[Cdecl]<IntPtr, IntPtr, IntPtr, void> callback) =>
            Api.duckdb_scalar_function_set_function(scalarFunction, callback);

        internal static DuckDBState DuckDBRegisterScalarFunction(DuckDBNativeConnection connection, IntPtr scalarFunction) =>
            Api.duckdb_register_scalar_function(connection.Handle, scalarFunction);

        internal static IntPtr DuckDBScalarFunctionGetExtraInfo(IntPtr scalarFunction) =>
            new(Api.duckdb_scalar_function_get_extra_info(scalarFunction));

        internal static void DuckDBScalarFunctionSetError(IntPtr info, string error)
        {
            fixed (byte* pointer = Helpers.ToUtf8(error))
            {
                Api.duckdb_scalar_function_set_error(info, pointer);
            }
        }
    }
}
