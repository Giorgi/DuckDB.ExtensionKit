using DuckDB.NET.Data;

namespace DuckDB.JWT.Tests;

[ClassDataSource<DuckDBExtensionFixture>(Shared = SharedType.PerAssembly)]
public class ExtractClaimsFromJwtTableFunctionTests(DuckDBExtensionFixture fixture)
{
    [Test]
    public async Task ExtractClaimsFromJwt_DefaultToken_ReturnsAllClaims()
    {
        var token = JwtTokenHelper.CreateDefaultToken();
        
        using var command = fixture.Connection.CreateCommand();
        command.CommandText = "SELECT * FROM extract_claims_from_jwt($token) ORDER BY claim_name";
        command.Parameters.Add(new DuckDBParameter("token", token));
        
        using var reader = command.ExecuteReader();
        var claims = new Dictionary<string, string>();
        
        while (reader.Read())
        {
            var claimName = reader.GetString(0);
            var claimValue = reader.GetString(1);
            claims[claimName] = claimValue;
        }
        
        await Assert.That(claims).Count().IsGreaterThanOrEqualTo(4);
        await Assert.That(claims).ContainsKey("sub");
        await Assert.That(claims["sub"]).IsEqualTo("1234567890");
        await Assert.That(claims).ContainsKey("name");
        await Assert.That(claims["name"]).IsEqualTo("John Doe");
        await Assert.That(claims).ContainsKey("admin");
        await Assert.That(claims["admin"]).IsEqualTo("true");
    }

    [Test]
    public async Task ExtractClaimsFromJwt_CustomClaims_ReturnsAllClaims()
    {
        var token = JwtTokenHelper.CreateToken(
            ("email", "user@example.com"),
            ("role", "developer"),
            ("department", "Engineering")
        );
        
        using var command = fixture.Connection.CreateCommand();
        command.CommandText = "SELECT * FROM extract_claims_from_jwt($token)";
        command.Parameters.Add(new DuckDBParameter("token", token));
        
        using var reader = command.ExecuteReader();
        var claims = new Dictionary<string, string>();
        
        while (reader.Read())
        {
            var claimName = reader.GetString(0);
            var claimValue = reader.GetString(1);
            claims[claimName] = claimValue;
        }
        
        await Assert.That(claims).ContainsKey("email");
        await Assert.That(claims["email"]).IsEqualTo("user@example.com");
        await Assert.That(claims).ContainsKey("role");
        await Assert.That(claims["role"]).IsEqualTo("developer");
        await Assert.That(claims).ContainsKey("department");
        await Assert.That(claims["department"]).IsEqualTo("Engineering");
    }

    [Test]
    public async Task ExtractClaimsFromJwt_FilterClaims_ReturnsFilteredResults()
    {
        var token = JwtTokenHelper.CreateDefaultToken();
        
        using var command = fixture.Connection.CreateCommand();
        command.CommandText = @"
            SELECT claim_name, claim_value 
            FROM extract_claims_from_jwt($token)
            WHERE claim_name = 'name'";
        command.Parameters.Add(new DuckDBParameter("token", token));
        
        using var reader = command.ExecuteReader();
        reader.Read();
        
        await Assert.That(reader.GetString(0)).IsEqualTo("name");
        await Assert.That(reader.GetString(1)).IsEqualTo("John Doe");
        
        var hasMore = reader.Read();
        await Assert.That(hasMore).IsFalse();
    }

    [Test]
    public async Task ExtractClaimsFromJwt_JoinWithOtherTable_Works()
    {
        var token = JwtTokenHelper.CreateDefaultToken();
        
        using var command = fixture.Connection.CreateCommand();
        command.CommandText = @"
            WITH expected_claims AS (
                SELECT 'name' as claim_name, 'John Doe' as expected_value
                UNION ALL
                SELECT 'sub', '1234567890'
            )
            SELECT 
                ec.claim_name,
                ec.expected_value,
                jwt.claim_value,
                ec.expected_value = jwt.claim_value as matches
            FROM expected_claims ec
            INNER JOIN extract_claims_from_jwt($token) jwt
                ON ec.claim_name = jwt.claim_name
            ORDER BY ec.claim_name";
        command.Parameters.Add(new DuckDBParameter("token", token));
        
        using var reader = command.ExecuteReader();
        var allMatch = true;
        var recordCount = 0;
        
        while (reader.Read())
        {
            recordCount++;
            var matches = reader.GetBoolean(3);
            if (!matches)
            {
                allMatch = false;
            }
        }
        
        await Assert.That(recordCount).IsEqualTo(2);
        await Assert.That(allMatch).IsTrue();
    }

    [Test]
    public async Task ExtractClaimsFromJwt_AggregateOperations_Work()
    {
        var token = JwtTokenHelper.CreateToken(
            ("role1", "admin"),
            ("role2", "user"),
            ("role3", "developer")
        );
        
        using var command = fixture.Connection.CreateCommand();
        command.CommandText = @"
            SELECT 
                COUNT(*) as total_claims,
                STRING_AGG(claim_name, ', ' ORDER BY claim_name) as all_claim_names
            FROM extract_claims_from_jwt($token)
            WHERE claim_name LIKE 'role%'";
        command.Parameters.Add(new DuckDBParameter("token", token));
        
        using var reader = command.ExecuteReader();
        reader.Read();
        
        var totalClaims = reader.GetInt64(0);
        var claimNames = reader.GetString(1);
        
        await Assert.That(totalClaims).IsEqualTo(3);
        await Assert.That(claimNames).Contains("role1");
        await Assert.That(claimNames).Contains("role2");
        await Assert.That(claimNames).Contains("role3");
    }
}
