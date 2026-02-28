using DuckDB.NET.Data;
using System.Globalization;

namespace DuckDB.JWT.Tests;

[ClassDataSource<DuckDBExtensionFixture>(Shared = SharedType.PerAssembly)]
public class ObjectTypeScalarFunctionTests(DuckDBExtensionFixture fixture) : IDisposable
{
    private readonly DuckDBConnection connection = fixture.CreateConnection();

    [Test]
    public async Task NetTypeName_Integer_ReturnsInt32()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT net_type_name(42)";

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo("Int32");
    }

    [Test]
    public async Task NetTypeName_BigInt_ReturnsInt64()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT net_type_name(42::BIGINT)";

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo("Int64");
    }

    [Test]
    public async Task NetTypeName_Varchar_ReturnsString()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT net_type_name('hello')";

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo("String");
    }

    [Test]
    public async Task NetTypeName_Boolean_ReturnsBoolean()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT net_type_name(true)";

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo("Boolean");
    }

    [Test]
    public async Task NetFormat_IntegerHex_ReturnsHexString()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT net_format(255, 'X')";

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo("FF");
    }

    [Test]
    public async Task NetFormat_DoublePercent_ReturnsPercentString()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT net_format(0.15, 'P')";

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo("15.00 %");
    }

    [Test]
    public async Task NetFormat_BigIntHex_ReturnsHexString()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT net_format(65535::BIGINT, 'X')";

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo("FFFF");
    }

    [Test]
    public async Task NetFormatCulture_GermanNumber_ReturnsGermanFormat()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT net_format_culture(1234.5, 'N2', 'de-DE')";

        var result = command.ExecuteScalar();
        var expected = 1234.5.ToString("N2", CultureInfo.GetCultureInfo("de-DE"));

        await Assert.That(result).IsEqualTo(expected);
    }

    [Test]
    public async Task NetFormatCulture_USNumber_ReturnsUSFormat()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT net_format_culture(1234.5, 'N2', 'en-US')";

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo("1,234.50");
    }

    [Test]
    public async Task NetConcat_MixedTypes_ReturnsConcatenatedString()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT net_concat(42, 'hello', true)";

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo("42, hello, True");
    }

    [Test]
    public async Task NetConcat_WithBoolean_ReturnsConcatenatedString()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT net_concat(true, 42::BIGINT)";

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo("True, 42");
    }

    [Test]
    public async Task NetConcat_SingleArg_ReturnsSingleValue()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT net_concat('only')";

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo("only");
    }

    public void Dispose() => connection.Dispose();
}
