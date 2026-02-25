using DuckDB.ExtensionKit;
using DuckDB.ExtensionKit.Native;
using DuckDB.ExtensionKit.ScalarFunctions;
using DuckDB.ExtensionKit.TableFunctions;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;

namespace DuckDB.JWT;

[DuckDBExtension]
public static partial class JwtExtension
{
    private static void RegisterFunctions(DuckDBConnection connection)
    {
        connection.RegisterScalarFunction<string, bool>("is_jwt", IsJwt);

        connection.RegisterScalarFunction<string, string, string?>("extract_claim_from_jwt", ExtractClaimFromJwt);

        connection.RegisterTableFunction("extract_claims_from_jwt", (string jwt) => ExtractClaimsFromJwt(jwt),
                                         c => new { claim_name = c.Key, claim_value = c.Value });

        connection.RegisterTableFunction("jwt_info", (string jwt) => GetJwtInfo(jwt),
                                         (JwtInfo info) => new { issuer = info.Issuer, claim_count = info.ClaimCount, is_expired = info.IsExpired });

        connection.RegisterTableFunction("extract_claims_limited", (string jwt, [Named("max_rows")] int? limit) => ExtractClaimsLimited(jwt, limit),
                                         c => new { claim_name = c.Key, claim_value = c.Value });

        connection.RegisterTableFunction("extract_claims_filtered", (string jwt, [Named] string? prefix, [Named("max_rows")] int? limit) => ExtractClaimsFiltered(jwt, prefix, limit),
                                         c => new { claim_name = c.Key, claim_value = c.Value });
    }

    private static bool IsJwt(string jwt)
    {
        if (string.IsNullOrWhiteSpace(jwt))
        {
            return false;
        }

        var jwtHandler = new JwtSecurityTokenHandler();

        if (!jwtHandler.CanReadToken(jwt))
        {
            return false;
        }

        try
        {
            var token = jwtHandler.ReadJwtToken(jwt);

            return token.Header != null && token.Payload != null;
        }
        catch
        {
            return false;
        }
    }

    private static string? ExtractClaimFromJwt(string jwt, string claim)
    {
        var jwtHandler = new JwtSecurityTokenHandler();
        var token = jwtHandler.ReadJwtToken(jwt);
        return token.Claims.FirstOrDefault(c => c.Type == claim)?.Value;
    }

    private static Dictionary<string, string> ExtractClaimsFromJwt(string jwt)
    {
        var jwtHandler = new JwtSecurityTokenHandler();
        var token = jwtHandler.ReadJwtToken(jwt);
        return token.Claims.ToDictionary(c => c.Type, c => c.Value);
    }

    private static Dictionary<string, string> ExtractClaimsLimited(string jwt, int? limit)
    {
        var jwtHandler = new JwtSecurityTokenHandler();
        var token = jwtHandler.ReadJwtToken(jwt);
        var claims = token.Claims.AsEnumerable();
        if (limit.HasValue)
            claims = claims.Take(limit.Value);
        return claims.ToDictionary(c => c.Type, c => c.Value);
    }

    private static Dictionary<string, string> ExtractClaimsFiltered(string jwt, string? prefix, int? limit)
    {
        var jwtHandler = new JwtSecurityTokenHandler();
        var token = jwtHandler.ReadJwtToken(jwt);
        var claims = token.Claims.AsEnumerable();
        if (prefix is not null)
            claims = claims.Where(c => c.Type.StartsWith(prefix));
        if (limit.HasValue)
            claims = claims.Take(limit.Value);
        return claims.ToDictionary(c => c.Type, c => c.Value);
    }

    private record JwtInfo(string Issuer, int ClaimCount, bool IsExpired);

    private static JwtInfo[] GetJwtInfo(string jwt)
    {
        var jwtHandler = new JwtSecurityTokenHandler();
        var token = jwtHandler.ReadJwtToken(jwt);
        return [new JwtInfo(
            token.Issuer ?? "",
            token.Claims.Count(),
            token.ValidTo < DateTime.UtcNow
        )];
    }
}