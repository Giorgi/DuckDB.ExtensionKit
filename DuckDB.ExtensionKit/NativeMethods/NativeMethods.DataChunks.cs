using DuckDB.ExtensionKit.Native;

namespace DuckDB.NET.Native;

public static partial class NativeMethods
{
    internal static unsafe class DataChunks
    {
        internal static long DuckDBDataChunkGetColumnCount(DuckDBDataChunk chunk) => (long)Api.duckdb_data_chunk_get_column_count(chunk.DangerousGetHandle());

        internal static IntPtr DuckDBDataChunkGetVector(DuckDBDataChunk chunk, long columnIndex) => Api.duckdb_data_chunk_get_vector(chunk.DangerousGetHandle(), (ulong)columnIndex);

        internal static ulong DuckDBDataChunkGetSize(DuckDBDataChunk chunk) => Api.duckdb_data_chunk_get_size(chunk.DangerousGetHandle());

        internal static void DuckDBDestroyDataChunk(IntPtr chunk) => Api.duckdb_destroy_data_chunk(&chunk);

        internal static void DuckDBDataChunkSetSize(DuckDBDataChunk chunk, ulong size) => Api.duckdb_data_chunk_set_size(chunk.DangerousGetHandle(), size);
    }
}