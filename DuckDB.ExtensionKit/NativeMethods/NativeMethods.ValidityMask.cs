namespace DuckDB.NET.Native;

public static partial class NativeMethods
{
    internal static unsafe class ValidityMask
    {
        internal static bool DuckDBValidityRowIsValid(ulong* validity, ulong row) => Api.duckdb_validity_row_is_valid(validity, row) != 0;

        internal static void DuckDBValiditySetRowValidity(ulong* validity, ulong row, bool valid) => Api.duckdb_validity_set_row_validity(validity, row, (byte)(valid ? 1 : 0));

        internal static void DuckDBValiditySetRowInvalid(ulong* validity, ulong row) => Api.duckdb_validity_set_row_invalid(validity, row);

        internal static void DuckDBValiditySetRowValid(ulong* validity, ulong row) => Api.duckdb_validity_set_row_valid(validity, row);
    }
}
