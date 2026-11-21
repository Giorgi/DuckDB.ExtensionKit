namespace DuckDB.NET.Native;

public static partial class NativeMethods
{
    internal static unsafe class Helpers
    {
        internal static void DuckDBFree(byte* ptr) => Api.duckdb_free(ptr);

        internal static void DuckDBFree(IntPtr ptr) => Api.duckdb_free(ptr.ToPointer());

        internal static ulong DuckDBVectorSize() => Api.duckdb_vector_size();
    }

    internal static unsafe class DateTimeHelpers
    {
        internal static DuckDBDateOnly DuckDBFromDate(DuckDBDate date) => Api.duckdb_from_date(date);

        internal static DuckDBDate DuckDBToDate(DuckDBDateOnly dateStruct) => Api.duckdb_to_date(dateStruct);

        internal static DuckDBTimeOnly DuckDBFromTime(DuckDBTime time) => Api.duckdb_from_time(time);

        internal static DuckDBTimeTz DuckDBFromTimeTz(DuckDBTimeTzStruct micros) => Api.duckdb_from_time_tz(micros);

        internal static DuckDBTimestamp DuckDBFromTimestamp(DuckDBTimestampStruct timestamp) => Api.duckdb_from_timestamp(timestamp);

        internal static DuckDBTimestampStruct DuckDBToTimestamp(DuckDBTimestamp dateStruct) => Api.duckdb_to_timestamp(dateStruct);
    }

    internal static unsafe class Query
    {
        // DuckDBResult is linked from DuckDB.NET, and its Close() calls this. The extension does not run queries.
        internal static void DuckDBDestroyResult(ref DuckDBResult result)
        {
            fixed (DuckDBResult* pointer = &result)
            {
                Api.duckdb_destroy_result((nint*)pointer);
            }
        }
    }
}
