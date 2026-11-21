using DuckDB.NET.Data;

namespace DuckDB.JWT.Tests;

// jwt_valid_from, jwt_valid_to and jwt_is_valid_at. The results are compared in SQL, through epoch() and
// to_timestamp(), so that they do not depend on how the client converts a TIMESTAMPTZ.
[ClassDataSource<DuckDBExtensionFixture>(Shared = SharedType.PerAssembly)]
public class JwtLifetimeFunctionTests(DuckDBExtensionFixture fixture) : IDisposable
{
    // 2023-11-14 22:13:20 UTC and 2023-11-15 22:13:20 UTC.
    private const long NotBefore = 1_700_000_000;
    private const long Expires = NotBefore + 86_400;

    private readonly DuckDBConnection connection = fixture.CreateConnection();

    [Test]
    public async Task JwtValidFrom_ReturnsNotBeforeAsTimestampTz()
    {
        var token = JwtTokenHelper.CreateTokenWithLifetime(NotBefore, Expires);

        await Assert.That(Scalar("SELECT epoch(jwt_valid_from($token))::BIGINT", token)).IsEqualTo(NotBefore);
        await Assert.That(Scalar("SELECT typeof(jwt_valid_from($token))", token)).IsEqualTo("TIMESTAMP WITH TIME ZONE");
    }

    [Test]
    public async Task JwtValidTo_ReturnsExpirationAsTimestampTz()
    {
        var token = JwtTokenHelper.CreateTokenWithLifetime(NotBefore, Expires);

        await Assert.That(Scalar("SELECT epoch(jwt_valid_to($token))::BIGINT", token)).IsEqualTo(Expires);
        await Assert.That(Scalar("SELECT typeof(jwt_valid_to($token))", token)).IsEqualTo("TIMESTAMP WITH TIME ZONE");
    }

    [Test]
    public async Task JwtValidTo_MatchesTimestampTzLiteral()
    {
        var token = JwtTokenHelper.CreateTokenWithLifetime(NotBefore, Expires);

        await Assert.That(Scalar("SELECT jwt_valid_to($token) = TIMESTAMPTZ '2023-11-15 22:13:20+00'", token)).IsEqualTo(true);
    }

    [Test]
    public async Task JwtValidFrom_WithoutNotBefore_ReturnsNull()
    {
        var token = JwtTokenHelper.CreateTokenWithLifetime(null, Expires);

        await Assert.That(Scalar("SELECT jwt_valid_from($token)", token)).IsEqualTo(DBNull.Value);
    }

    [Test]
    public async Task JwtValidTo_WithoutExpiration_ReturnsNull()
    {
        var token = JwtTokenHelper.CreateTokenWithLifetime(NotBefore, null);

        await Assert.That(Scalar("SELECT jwt_valid_to($token)", token)).IsEqualTo(DBNull.Value);
    }

    [Test]
    public async Task LifetimeFunctions_NullToken_ReturnNull()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT jwt_valid_from(NULL) IS NULL AND jwt_valid_to(NULL) IS NULL AND jwt_is_valid_at(NULL, now()) IS NULL";

        await Assert.That(command.ExecuteScalar()).IsEqualTo(true);
    }

    [Test]
    [Arguments(NotBefore - 1, false)]
    [Arguments(NotBefore, true)]
    [Arguments(NotBefore + 3_600, true)]
    [Arguments(Expires - 1, true)]
    [Arguments(Expires, false)]
    [Arguments(Expires + 1, false)]
    public async Task JwtIsValidAt_FromNotBeforeUntilExpiration(long at, bool expected)
    {
        var token = JwtTokenHelper.CreateTokenWithLifetime(NotBefore, Expires);

        await Assert.That(IsValidAt(token, at)).IsEqualTo(expected);
    }

    [Test]
    [Arguments(null, null, 0L, true)]
    [Arguments(null, Expires, 0L, true)]
    [Arguments(null, Expires, Expires, false)]
    [Arguments(NotBefore, null, NotBefore - 1, false)]
    [Arguments(NotBefore, null, 4_000_000_000L, true)]
    public async Task JwtIsValidAt_MissingClaimLeavesThatSideOpen(long? notBefore, long? expires, long at, bool expected)
    {
        var token = JwtTokenHelper.CreateTokenWithLifetime(notBefore, expires);

        await Assert.That(IsValidAt(token, at)).IsEqualTo(expected);
    }

    [Test]
    public async Task JwtIsValidAt_ComparesInstantsWhateverTheOffset()
    {
        var token = JwtTokenHelper.CreateTokenWithLifetime(NotBefore, Expires);

        // The same instants as NotBefore and one second before it, written at UTC+02.
        await Assert.That(Scalar("SELECT jwt_is_valid_at($token, TIMESTAMPTZ '2023-11-15 00:13:20+02')", token)).IsEqualTo(true);
        await Assert.That(Scalar("SELECT jwt_is_valid_at($token, TIMESTAMPTZ '2023-11-15 00:13:19+02')", token)).IsEqualTo(false);
    }

    [Test]
    public async Task JwtIsValidAt_OverManyRows_CountsTheSecondsOfTheLifetime()
    {
        var token = JwtTokenHelper.CreateTokenWithLifetime(NotBefore, NotBefore + 5_000);

        // More rows than one vector holds (2048), so the function runs over several chunks.
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT count(*) FILTER (WHERE jwt_is_valid_at($token, to_timestamp(s)))
            FROM range($from, $to) t(s)
            """;
        command.Parameters.Add(new DuckDBParameter("token", token));
        command.Parameters.Add(new DuckDBParameter("from", NotBefore - 1_000));
        command.Parameters.Add(new DuckDBParameter("to", NotBefore + 6_000));

        await Assert.That(command.ExecuteScalar()).IsEqualTo(5_000L);
    }

    private object? IsValidAt(string token, long at)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT jwt_is_valid_at($token, to_timestamp($at))";
        command.Parameters.Add(new DuckDBParameter("token", token));
        command.Parameters.Add(new DuckDBParameter("at", at));

        return command.ExecuteScalar();
    }

    private object? Scalar(string sql, string token)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add(new DuckDBParameter("token", token));

        return command.ExecuteScalar();
    }

    public void Dispose() => connection.Dispose();
}
