using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using Microsoft.IdentityModel.Tokens;

namespace DuckDB.JWT.Tests;

public static class JwtTokenHelper
{
    private static readonly byte[] SecretKey = "ThisIsAVerySecretKeyForTestingPurposesOnly123456789"u8.ToArray();

    public static string CreateToken(params (string Type, string Value)[] claims)
    {
        var securityKey = new SymmetricSecurityKey(SecretKey);
        var credentials = new SigningCredentials(securityKey, SecurityAlgorithms.HmacSha256);

        var claimsList = claims.Select(c => new Claim(c.Type, c.Value)).ToList();
        
        var token = new JwtSecurityToken(
            issuer: "test-issuer",
            audience: "test-audience",
            claims: claimsList,
            expires: DateTime.UtcNow.AddHours(1),
            signingCredentials: credentials
        );

        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    public static string CreateDefaultToken()
    {
        return CreateToken(
            ("sub", "1234567890"),
            ("name", "John Doe"),
            ("admin", "true"),
            ("iat", "1516239022")
        );
    }
}
