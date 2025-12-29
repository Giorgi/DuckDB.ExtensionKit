using DuckDB.NET.Data;

namespace DuckDB.JWT.Tests;

[ClassDataSource<DuckDBExtensionFixture>(Shared = SharedType.PerAssembly)]
public class ExtractClaimFromJwtFunctionTests(DuckDBExtensionFixture fixture)
{
    [Test]
    public async Task ExtractClaimFromJwt_ValidClaim_ReturnsValue()
    {
        var token = JwtTokenHelper.CreateDefaultToken();
        
        using var command = fixture.Connection.CreateCommand();
        command.CommandText = "SELECT extract_claim_from_jwt($token, $claim) as result";
        command.Parameters.Add(new DuckDBParameter("token", token));
        command.Parameters.Add(new DuckDBParameter("claim", "name"));
        
        var result = command.ExecuteScalar();
        
        await Assert.That(result).IsEqualTo("John Doe");
    }

    [Test]
    public async Task ExtractClaimFromJwt_SubjectClaim_ReturnsValue()
    {
        var token = JwtTokenHelper.CreateDefaultToken();
        
        using var command = fixture.Connection.CreateCommand();
        command.CommandText = "SELECT extract_claim_from_jwt($token, $claim) as result";
        command.Parameters.Add(new DuckDBParameter("token", token));
        command.Parameters.Add(new DuckDBParameter("claim", "sub"));
        
        var result = command.ExecuteScalar();
        
        await Assert.That(result).IsEqualTo("1234567890");
    }

    [Test]
    public async Task ExtractClaimFromJwt_AdminClaim_ReturnsValue()
    {
        var token = JwtTokenHelper.CreateDefaultToken();
        
        using var command = fixture.Connection.CreateCommand();
        command.CommandText = "SELECT extract_claim_from_jwt($token, $claim) as result";
        command.Parameters.Add(new DuckDBParameter("token", token));
        command.Parameters.Add(new DuckDBParameter("claim", "admin"));
        
        var result = command.ExecuteScalar();
        
        await Assert.That(result).IsEqualTo("true");
    }

    [Test]
    public async Task ExtractClaimFromJwt_NonExistentClaim_ReturnsNull()
    {
        var token = JwtTokenHelper.CreateDefaultToken();
        
        using var command = fixture.Connection.CreateCommand();
        command.CommandText = "SELECT extract_claim_from_jwt($token, $claim) as result";
        command.Parameters.Add(new DuckDBParameter("token", token));
        command.Parameters.Add(new DuckDBParameter("claim", "nonexistent"));
        
        var result = command.ExecuteScalar();

        await Assert.That(result).IsEqualTo(DBNull.Value);
    }

    [Test]
    public async Task ExtractClaimFromJwt_CustomClaims_ReturnsValues()
    {
        var token = JwtTokenHelper.CreateToken(
            ("email", "test@example.com"),
            ("role", "admin"),
            ("department", "IT")
        );
        
        using var command = fixture.Connection.CreateCommand();
        command.CommandText = @"
            SELECT 
                extract_claim_from_jwt($token, $email_claim) as email,
                extract_claim_from_jwt($token, $role_claim) as role,
                extract_claim_from_jwt($token, $dept_claim) as department";
        command.Parameters.Add(new DuckDBParameter("token", token));
        command.Parameters.Add(new DuckDBParameter("email_claim", "email"));
        command.Parameters.Add(new DuckDBParameter("role_claim", "role"));
        command.Parameters.Add(new DuckDBParameter("dept_claim", "department"));
        
        using var reader = command.ExecuteReader();
        reader.Read();
        
        await Assert.That(reader.GetString(0)).IsEqualTo("test@example.com");
        await Assert.That(reader.GetString(1)).IsEqualTo("admin");
        await Assert.That(reader.GetString(2)).IsEqualTo("IT");
    }

    [Test]
    public async Task ExtractClaimFromJwt_MultipleTokens_ReturnsCorrectValues()
    {
        var token1 = JwtTokenHelper.CreateToken(("name", "Alice"));
        var token2 = JwtTokenHelper.CreateToken(("name", "Bob"));
        
        using var command = fixture.Connection.CreateCommand();
        command.CommandText = @"
            SELECT extract_claim_from_jwt(token, $claim) as name
            FROM (VALUES ($token1), ($token2)) AS t(token)";
        command.Parameters.Add(new DuckDBParameter("token1", token1));
        command.Parameters.Add(new DuckDBParameter("token2", token2));
        command.Parameters.Add(new DuckDBParameter("claim", "name"));
        
        using var reader = command.ExecuteReader();
        var names = new List<string>();
        
        while (reader.Read())
        {
            names.Add(reader.GetString(0));
        }
        
        await Assert.That(names).Count().IsEqualTo(2);
        await Assert.That(names[0]).IsEqualTo("Alice");
        await Assert.That(names[1]).IsEqualTo("Bob");
    }
}
