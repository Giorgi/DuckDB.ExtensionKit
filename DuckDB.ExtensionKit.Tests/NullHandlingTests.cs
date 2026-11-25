namespace DuckDB.ExtensionKit.Tests;

[ClassDataSource<ExtensionFixture>(Shared = SharedType.PerAssembly)]
public class NullHandlingTests(ExtensionFixture fixture) : ExtensionTestBase(fixture)
{
    // Without a nullable parameter, DuckDB answers a constant NULL itself, but calls the function with the whole chunk
    // of a column, NULL rows included. Such a row gets NULL, and the function is not called for it.
    [Test]
    public async Task NullRowOfAColumnGetsNullWithoutNullableParameter()
    {
        var rows = Rows("SELECT describe_sum(a, b) FROM (VALUES (1, 1, 2), (2, NULL, 2), (3, 3, NULL), (4, 4, 5)) t(o, a, b) ORDER BY o");

        await Assert.That(rows.Select(r => r[0]).ToList()).IsEquivalentTo(new object?[] { "sum 3", null, null, "sum 9" });
    }

    [Test]
    public async Task ConstantNullGetsNull()
    {
        await Assert.That(Scalar("SELECT describe_sum(NULL, 1) IS NULL")).IsEqualTo(true);
    }

    [Test]
    public async Task NullRowsOverManyChunks()
    {
        var counts = Rows("""
            SELECT count(*) FILTER (WHERE r IS NULL), count(*) FILTER (WHERE r = 'sum ' || (i + 1))
            FROM (SELECT i, describe_sum(CASE WHEN i % 3 = 0 THEN NULL ELSE i END::INTEGER, 1) AS r FROM range(5000) t(i))
            """)[0];

        await Assert.That(counts[0]).IsEqualTo(1667L);
        await Assert.That(counts[1]).IsEqualTo(3333L);
    }

    [Test]
    public async Task NullableParameterIsCalledForNull()
    {
        var rows = Rows("SELECT describe_nullable(x) FROM (VALUES (1, 7), (2, NULL)) t(o, x) ORDER BY o");

        await Assert.That(rows.Select(r => r[0]).ToList()).IsEquivalentTo(new object?[] { "value 7", "no value" });
    }

    [Test]
    public async Task ParamsFunctionTakesAnyNumberOfArguments()
    {
        await Assert.That(Scalar("SELECT sum_all(1, 2, 3, 4)")).IsEqualTo(10);
        await Assert.That(Scalar("SELECT sum_all(5)")).IsEqualTo(5);
        await Assert.That(Scalar("SELECT sum_all(1, NULL, 3) IS NULL")).IsEqualTo(true);
    }
}
