using DuckDB.NET.Data;

namespace DuckDB.JWT.Tests;

[ClassDataSource<DuckDBExtensionFixture>(Shared = SharedType.PerAssembly)]
public class HandlesNullsScalarFunctionTests(DuckDBExtensionFixture fixture) : IDisposable
{
    private readonly DuckDBConnection connection = fixture.CreateConnection();

    [Test]
    public async Task DescribeVal_WithNull_ReturnsNothing()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT describe_val(NULL::INT)";

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo("nothing");
    }

    [Test]
    public async Task DescribeVal_WithValue_ReturnsValueString()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT describe_val(42)";

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo("42");
    }

    [Test]
    public async Task CoalesceAdd_NullableParamIsNull_UsesSecondParam()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT coalesce_add(NULL::INT, 5)";

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo("5");
    }

    [Test]
    public async Task CoalesceAdd_BothPresent_AddsValues()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT coalesce_add(10, 5)";

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo("15");
    }

    public void Dispose() => connection.Dispose();
}
