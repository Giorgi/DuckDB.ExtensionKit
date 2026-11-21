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

    // A token with the given nbf and exp, in seconds since the Unix epoch; null leaves the claim out. The claims are
    // set on the payload directly: the JwtSecurityToken constructor drops nbf when it is given no expiry.
    public static string CreateTokenWithLifetime(long? notBefore, long? expires)
    {
        var credentials = new SigningCredentials(new SymmetricSecurityKey(SecretKey), SecurityAlgorithms.HmacSha256);

        var payload = new JwtPayload(issuer: "test-issuer", audience: "test-audience",
                                     claims: [new Claim("sub", "1234567890")], notBefore: null, expires: null);

        if (notBefore.HasValue)
        {
            payload[JwtRegisteredClaimNames.Nbf] = notBefore.Value;
        }

        if (expires.HasValue)
        {
            payload[JwtRegisteredClaimNames.Exp] = expires.Value;
        }

        return new JwtSecurityTokenHandler().WriteToken(new JwtSecurityToken(new JwtHeader(credentials), payload));
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
