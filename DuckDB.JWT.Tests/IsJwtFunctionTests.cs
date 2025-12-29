using DuckDB.NET.Data;

namespace DuckDB.JWT.Tests;

[ClassDataSource<DuckDBExtensionFixture>(Shared = SharedType.PerAssembly)]
public class IsJwtFunctionTests(DuckDBExtensionFixture fixture)
{
    [Test]
    public async Task IsJwt_ValidToken_ReturnsTrue()
    {
        var token = JwtTokenHelper.CreateDefaultToken();
        
        using var command = fixture.Connection.CreateCommand();
        command.CommandText = "SELECT is_jwt($token) as result";
        command.Parameters.Add(new DuckDBParameter("token", token));
        
        var result = command.ExecuteScalar();
        
        await Assert.That(result).IsEqualTo(true);
    }

    [Test]
    public async Task IsJwt_InvalidToken_ReturnsFalse()
    {
        const string invalidToken = "not.a.jwt";
        
        using var command = fixture.Connection.CreateCommand();
        command.CommandText = "SELECT is_jwt($token) as result";
        command.Parameters.Add(new DuckDBParameter("token", invalidToken));
        
        var result = command.ExecuteScalar();
        
        await Assert.That(result).IsEqualTo(false);
    }

    [Test]
    public async Task IsJwt_EmptyString_ReturnsFalse()
    {
        const string emptyToken = "";
        
        using var command = fixture.Connection.CreateCommand();
        command.CommandText = "SELECT is_jwt($token) as result";
        command.Parameters.Add(new DuckDBParameter("token", emptyToken));
        
        var result = command.ExecuteScalar();
        
        await Assert.That(result).IsEqualTo(false);
    }

    [Test]
    public async Task IsJwt_MultipleTokesInTable_ReturnsCorrectResults()
    {
        var validToken = JwtTokenHelper.CreateDefaultToken();
        
        using var command = fixture.Connection.CreateCommand();
        command.CommandText = @"
            SELECT is_jwt(token) as result 
            FROM (VALUES 
                ($validToken), 
                ('invalid.token'), 
                ('') 
            ) AS t(token)";
        command.Parameters.Add(new DuckDBParameter("validToken", validToken));
        
        using var reader = command.ExecuteReader();
        var results = new List<bool>();
        
        while (reader.Read())
        {
            results.Add(reader.GetBoolean(0));
        }
        
        await Assert.That(results).Count().IsEqualTo(3);
        await Assert.That(results[0]).IsTrue();
        await Assert.That(results[1]).IsFalse();
        await Assert.That(results[2]).IsFalse();
    }
}
