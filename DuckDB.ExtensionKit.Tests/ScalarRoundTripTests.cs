namespace DuckDB.ExtensionKit.Tests;

// A value read as a .NET type by a scalar function and written back as it comes back unchanged, with the DuckDB type
// that the .NET type maps to.
[ClassDataSource<ExtensionFixture>(Shared = SharedType.PerAssembly)]
public class ScalarRoundTripTests(ExtensionFixture fixture) : ExtensionTestBase(fixture)
{
    [Test]
    [Arguments("echo_bool", "true", "BOOLEAN")]
    [Arguments("echo_sbyte", "(-5)::TINYINT", "TINYINT")]
    [Arguments("echo_short", "(-300)::SMALLINT", "SMALLINT")]
    [Arguments("echo_int", "(-70000)::INTEGER", "INTEGER")]
    [Arguments("echo_long", "(-5000000000)::BIGINT", "BIGINT")]
    [Arguments("echo_byte", "200::UTINYINT", "UTINYINT")]
    [Arguments("echo_ushort", "60000::USMALLINT", "USMALLINT")]
    [Arguments("echo_uint", "4000000000::UINTEGER", "UINTEGER")]
    [Arguments("echo_ulong", "18000000000000000000::UBIGINT", "UBIGINT")]
    [Arguments("echo_float", "1.5::FLOAT", "FLOAT")]
    [Arguments("echo_double", "2.25::DOUBLE", "DOUBLE")]
    [Arguments("echo_decimal", "12345.678901234567890123::DECIMAL(38,18)", "DECIMAL(38,18)")]
    [Arguments("echo_int128", "(-170141183460469231731687303715884105727)::HUGEINT", "HUGEINT")]
    [Arguments("echo_uint128", "340282366920938463463374607431768211455::UHUGEINT", "UHUGEINT")]
    [Arguments("echo_biginteger", "170141183460469231731687303715884105727::HUGEINT", "HUGEINT")]
    [Arguments("echo_string", "'short'", "VARCHAR")]
    [Arguments("echo_string", "'a string longer than twelve bytes, so not inlined'", "VARCHAR")]
    [Arguments("echo_string", "'ünïcödé ✓'", "VARCHAR")]
    [Arguments("echo_guid", "'6ba7b810-9dad-11d1-80b4-00c04fd430c8'::UUID", "UUID")]
    [Arguments("echo_dateonly", "DATE '2024-02-29'", "DATE")]
    [Arguments("echo_dateonly", "DATE '1969-07-20'", "DATE")]
    [Arguments("echo_timeonly", "TIME '13:45:30.123456'", "TIME")]
    [Arguments("echo_duckdbdateonly", "DATE '2024-02-29'", "DATE")]
    [Arguments("echo_duckdbtimeonly", "TIME '13:45:30.123456'", "TIME")]
    [Arguments("echo_datetime", "TIMESTAMP '2024-02-29 13:45:30.123456'", "TIMESTAMP")]
    [Arguments("echo_datetimeoffset", "TIMESTAMPTZ '2024-02-29 13:45:30.123456+00'", "TIMESTAMP WITH TIME ZONE")]
    [Arguments("echo_timespan", "INTERVAL '1 day 02:03:04.5'", "INTERVAL")]
    [Arguments("echo_bytes", "'\\x00\\xFF\\x01'::BLOB", "BLOB")]
    [Arguments("echo_bytes", "''::BLOB", "BLOB")]
    [Arguments("echo_list_int", "[1, 2, 3]", "INTEGER[]")]
    [Arguments("echo_list_string", "['x', NULL, 'z']", "VARCHAR[]")]
    [Arguments("echo_list_nullable_int", "[1, NULL, 3]", "INTEGER[]")]
    [Arguments("echo_list_nullable_datetime", "[TIMESTAMP '2024-02-29 13:45:30.5', NULL]", "TIMESTAMP[]")]
    [Arguments("echo_list_decimal", "[1.25, 2.5]::DECIMAL(38,18)[]", "DECIMAL(38,18)[]")]
    [Arguments("echo_list_guid", "['6ba7b810-9dad-11d1-80b4-00c04fd430c8'::UUID]", "UUID[]")]
    [Arguments("echo_nullable_int", "42", "INTEGER")]
    [Arguments("echo_nullable_int", "NULL::INTEGER", "INTEGER")]
    [Arguments("echo_nullable_datetime", "TIMESTAMP '2024-02-29 13:45:30'", "TIMESTAMP")]
    [Arguments("echo_nullable_datetime", "NULL::TIMESTAMP", "TIMESTAMP")]
    [Arguments("echo_nullable_int128", "(-1)::HUGEINT", "HUGEINT")]
    [Arguments("echo_nullable_int128", "NULL::HUGEINT", "HUGEINT")]
    [Arguments("echo_nullable_guid", "'6ba7b810-9dad-11d1-80b4-00c04fd430c8'::UUID", "UUID")]
    [Arguments("echo_nullable_guid", "NULL::UUID", "UUID")]
    public async Task ValueComesBackUnchanged(string function, string literal, string duckDBType)
    {
        await Assert.That(Scalar($"SELECT {function}({literal}) IS NOT DISTINCT FROM {literal}")).IsEqualTo(true);
        await Assert.That(Scalar($"SELECT typeof({function}({literal}))")).IsEqualTo(duckDBType);
    }

    // Over a column, so that the function runs over several vectors and both short and long strings.
    [Test]
    [Arguments("echo_int", "i::INTEGER")]
    [Arguments("echo_string", "repeat('x', (i % 40)::INTEGER)")]
    [Arguments("echo_datetime", "TIMESTAMP '2024-01-01' + to_seconds(i)")]
    [Arguments("echo_decimal", "(i / 7)::DECIMAL(38,18)")]
    public async Task ColumnComesBackUnchanged(string function, string expression)
    {
        var mismatches = Scalar($"SELECT count(*) FILTER (WHERE {function}({expression}) IS DISTINCT FROM {expression}) FROM range(5000) t(i)");

        await Assert.That(mismatches).IsEqualTo(0L);
    }

    [Test]
    public async Task ListWithNullNeedsNullableElementType()
    {
        await Assert.That(() => Scalar("SELECT echo_list_int([1, NULL, 3])")).Throws<Exception>().WithMessageContaining("null");
    }

    [Test]
    public async Task ByteArrayResultIsBlob()
    {
        await Assert.That(Scalar("SELECT blob_of('abc') = 'abc'::BLOB")).IsEqualTo(true);
        await Assert.That(Scalar("SELECT octet_length(blob_of(''))")).IsEqualTo(0L);
        await Assert.That(Scalar("SELECT blob_of('none') IS NULL")).IsEqualTo(true);
    }

    [Test]
    [Arguments("42::INTEGER", "Int32")]
    [Arguments("42::HUGEINT", "Int128")]
    [Arguments("'x'::BLOB", "Byte[]")]
    [Arguments("DATE '2024-02-29'", "DateOnly")]
    [Arguments("TIMESTAMP_NS '2024-02-29 13:45:30.123456789'", "DateTime")]
    [Arguments("TIMETZ '13:45:30+02'", "DateTimeOffset")]
    [Arguments("INTERVAL '2 hours'", "TimeSpan")]
    [Arguments("'b'::ENUM('a', 'b')", "String")]
    [Arguments("[1, 2]", "List`1")]
    [Arguments("MAP {'k': 1}", "Dictionary`2")]
    public async Task ObjectParameterGetsTheClrTypeOfTheColumn(string literal, string clrType)
    {
        await Assert.That(Scalar($"SELECT clr_type({literal})")).IsEqualTo(clrType);
    }
}
