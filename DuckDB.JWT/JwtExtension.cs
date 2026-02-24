using DuckDB.ExtensionKit;
using DuckDB.ExtensionKit.Native;
using DuckDB.ExtensionKit.ScalarFunctions;
using DuckDB.ExtensionKit.TableFunctions;
using System.IdentityModel.Tokens.Jwt;

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
}