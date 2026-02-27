using DuckDB.NET.Data;

namespace DuckDB.JWT.Tests;

[ClassDataSource<DuckDBExtensionFixture>(Shared = SharedType.PerClass)]
public class HasJwtClaimsFunctionTests(DuckDBExtensionFixture fixture)
{
    [Test]
    public async Task HasJwtClaims_AllClaimsPresent_ReturnsTrue()
    {
        var token = JwtTokenHelper.CreateDefaultToken();

        using var command = fixture.Connection.CreateCommand();
        command.CommandText = "SELECT has_jwt_claims($token, 'sub', 'name', 'admin')";
        command.Parameters.Add(new DuckDBParameter("token", token));

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo(true);
    }

    [Test]
    public async Task HasJwtClaims_MissingClaim_ReturnsFalse()
    {
        var token = JwtTokenHelper.CreateDefaultToken();

        using var command = fixture.Connection.CreateCommand();
        command.CommandText = "SELECT has_jwt_claims($token, 'sub', 'nonexistent')";
        command.Parameters.Add(new DuckDBParameter("token", token));

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo(false);
    }

    [Test]
    public async Task HasJwtClaims_SingleClaim_ReturnsTrue()
    {
        var token = JwtTokenHelper.CreateDefaultToken();

        using var command = fixture.Connection.CreateCommand();
        command.CommandText = "SELECT has_jwt_claims($token, 'name')";
        command.Parameters.Add(new DuckDBParameter("token", token));

        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo(true);
    }

    [Test]
    public async Task HasJwtClaims_CustomClaims_ReturnsCorrectResults()
    {
        var token = JwtTokenHelper.CreateToken(
            ("email", "test@example.com"),
            ("role", "admin"),
            ("department", "IT")
        );

        using var command = fixture.Connection.CreateCommand();
        command.CommandText = @"
            SELECT
                has_jwt_claims($token, 'email', 'role') as has_both,
                has_jwt_claims($token, 'email', 'missing') as has_one_missing";
        command.Parameters.Add(new DuckDBParameter("token", token));

        using var reader = command.ExecuteReader();
        reader.Read();

        await Assert.That(reader.GetBoolean(0)).IsTrue();
        await Assert.That(reader.GetBoolean(1)).IsFalse();
    }
}
