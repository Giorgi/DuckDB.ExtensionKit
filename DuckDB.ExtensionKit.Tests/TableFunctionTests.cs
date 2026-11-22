namespace DuckDB.ExtensionKit.Tests;

[ClassDataSource<ExtensionFixture>(Shared = SharedType.PerAssembly)]
public class TableFunctionTests(ExtensionFixture fixture) : ExtensionTestBase(fixture)
{
    // Every column type over 3,000 rows, more than one vector holds. Row i is made by MakeRow in the test extension.
    [Test]
    public async Task EveryColumnTypeOverSeveralChunks()
    {
        var mismatches = Scalar("""
            SELECT count(*) FILTER (WHERE NOT (
                even = (id % 2 = 0)
                AND big = id::BIGINT * 1000000000
                AND price = 1.5 + id
                AND ts = TIMESTAMP '2024-02-29 13:45:30' + to_days(id)
                AND ts_utc = TIMESTAMPTZ '2024-02-29 13:45:30+00' + to_hours(id)
                AND span = to_minutes(id)
                AND day = DATE '2024-02-29' + id
                AND time = TIME '13:45:30' + to_seconds(id)
                AND huge = id::HUGEINT * 9223372036854775807
                AND name = 'row ' || id
                AND bytes = '\x01\x02'::BLOB || unhex(lpad(to_hex(id % 256), 2, '0'))
                AND (maybe IS NULL) = (id % 3 = 0)))
            FROM rows_of(3000)
            """);

        await Assert.That(mismatches).IsEqualTo(0L);
        await Assert.That(Scalar("SELECT count(*) FROM rows_of(3000)")).IsEqualTo(3000L);
    }

    [Test]
    public async Task ColumnTypes()
    {
        var types = Rows("SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM rows_of(1))")
            .ToDictionary(r => (string)r[0]!, r => (string)r[1]!);

        await Assert.That(types["even"]).IsEqualTo("BOOLEAN");
        await Assert.That(types["price"]).IsEqualTo("DECIMAL(38,18)");
        await Assert.That(types["key"]).IsEqualTo("UUID");
        await Assert.That(types["ts_utc"]).IsEqualTo("TIMESTAMP WITH TIME ZONE");
        await Assert.That(types["span"]).IsEqualTo("INTERVAL");
        await Assert.That(types["huge"]).IsEqualTo("HUGEINT");
        await Assert.That(types["bytes"]).IsEqualTo("BLOB");
        await Assert.That(types["maybe"]).IsEqualTo("INTEGER");
    }

    // Decimal arithmetic and a comparison in the projection: under Native AOT this needs decimal's operator methods,
    // which the trimmer used to remove.
    [Test]
    public async Task DecimalArithmeticInProjection()
    {
        var result = Rows("""
            SELECT count(*), count(*) FILTER (WHERE total = (1.5 + id) * 2 + 0.25 AND discounted = 0.5 + id),
                   count(*) FILTER (WHERE expensive)
            FROM decimal_projection(3000)
            """)[0];

        await Assert.That(result[0]).IsEqualTo(3000L);
        await Assert.That(result[1]).IsEqualTo(3000L);
        await Assert.That(result[2]).IsEqualTo(2901L);
    }

    [Test]
    public async Task TimestampArgument()
    {
        var rows = Rows("SELECT day::VARCHAR FROM days_from(TIMESTAMP '2024-02-28 10:00:00', 3)");

        await Assert.That(rows.Select(r => r[0]).ToList())
            .IsEquivalentTo(new object?[] { "2024-02-28 10:00:00", "2024-02-29 10:00:00", "2024-03-01 10:00:00" });
    }

    [Test]
    public async Task DateTimeAndIntervalArguments()
    {
        var row = Rows("""
            SELECT day = DATE '2024-02-29', time = TIME '13:45:30.5', span = INTERVAL '90 minutes',
                   ts = TIMESTAMP '2024-02-29 13:45:30'
            FROM temporal_args(DATE '2024-02-29', TIME '13:45:30.5', INTERVAL '90 minutes', TIMESTAMP '2024-02-29 13:45:30')
            """)[0];

        await Assert.That(row).IsEquivalentTo(new object?[] { true, true, true, true });
    }

    [Test]
    public async Task NamedArguments()
    {
        await Assert.That(Rows("SELECT value FROM named_args(2)").Select(r => r[0]).ToList())
            .IsEquivalentTo(new object?[] { "item 0", "item 1" });
        await Assert.That(Rows("SELECT value FROM named_args(3, prefix := 'n', step := 10)").Select(r => r[0]).ToList())
            .IsEquivalentTo(new object?[] { "n 0", "n 10", "n 20" });
    }

    // The data callback is told which columns the query reads. The order is DuckDB's, so only the set is checked.
    [Test]
    public async Task DataCallbackGetsTheProjectedColumns()
    {
        await Assert.That(Scalar("SELECT b FROM projected_columns()")).IsEqualTo("b");
        await Assert.That(Rows("SELECT list_sort(string_split(c, ','))::VARCHAR, a FROM projected_columns()")[0][0]).IsEqualTo("[a, c]");
        await Assert.That(Scalar("SELECT list_sort(string_split(a, ','))::VARCHAR FROM projected_columns()")).IsEqualTo("[a]");
    }

    // The low-level registration, with the parameter types given as DuckDB types.
    [Test]
    public async Task ParametersGivenAsDuckDBTypes()
    {
        var rows = Rows("SELECT text, n FROM repeat_text('hi', 3)");

        await Assert.That(rows.Select(r => $"{r[0]}{r[1]}").ToList()).IsEquivalentTo(new[] { "hi1", "hi2", "hi3" });
    }

    // DuckDB scans a table function again without binding it again, on every iteration of a recursive CTE.
    [Test]
    public async Task ScannedAgainInRecursiveQuery()
    {
        var count = Scalar("""
            WITH RECURSIVE r(level, n) AS (
                SELECT 1, (SELECT count(*) FROM rows_of(10))
                UNION ALL
                SELECT level + 1, (SELECT count(*) FROM rows_of(10)) FROM r WHERE level < 3)
            SELECT sum(n)::BIGINT FROM r
            """);

        await Assert.That(count).IsEqualTo(30L);
    }
}
