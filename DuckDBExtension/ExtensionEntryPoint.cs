using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace DuckDB.Extension;

// ============================================================================
// Entry Point - Export this from your NativeAOT DLL
// ============================================================================
public static unsafe partial class ExtensionEntryPoint
{
    // Global API instance - populated on init
    private static DuckDBExtApiV1 Api;

    [UnmanagedCallersOnly(EntryPoint = "dotnetextension_init_c_api", CallConvs = [typeof(CallConvCdecl)])]
    public static byte Init(nint info, DuckDBExtensionAccess* access)
    {
        try
        {
            nint apiPtr;

            // Get the API vtable (version 1.2.0)
            var version = "v1.2.0\0"u8;
            fixed (byte* versionPtr = version)
            {
                apiPtr = access->GetApi(info, versionPtr);
            }

            if (apiPtr == 0)
                return 0;

            // Copy the API struct
            Api = *(DuckDBExtApiV1*)apiPtr;

            // Get database and open connection
            nint* dbPtr = access->GetDatabase(info);
            nint connection;
            if (Api.duckdb_connect(*dbPtr, &connection) != DuckDBState.Success)
            {
                SetError(access, info, "Failed to connect to database");
                return 0;
            }

            NativeMethods.Api = Api;

            // Register your functions here
            RegisterFunctions(new DuckDBNativeConnection(connection));

            // Cleanup
            Api.duckdb_disconnect(&connection);
            return 1; // success
        }
        catch(Exception e)
        {
            File.WriteAllText(@"D:\\err.txt", e.ToString());
            return 0;
        }
    }

    private static void SetError(DuckDBExtensionAccess* access, nint info, string message)
    {
        var bytes = Encoding.UTF8.GetBytes(message + '\0');
        fixed (byte* ptr = bytes)
        {
            access->SetError(info, ptr);
        }
    }
}