using System.IdentityModel.Tokens.Jwt;

namespace DuckDB.Extension;

public static partial class ExtensionEntryPoint
{
    private static void RegisterFunctions(DuckDBNativeConnection connection)
    {
        connection.RegisterScalarFunction<string, bool>("is_jwt", (readers, writer, rowCount) =>
        {
            for (ulong index = 0; index < rowCount; index++)
            {
                var value = readers[0].GetValue<string>(index);
                var isJwt = IsJwt(value);

                writer.WriteValue(isJwt, index);
            }
        });

        connection.RegisterScalarFunction<string, string, string>("extract_claim_from_jwt", (readers, writer, rowCount) =>
        {
            for (ulong index = 0; index < rowCount; index++)
            {
                var jwt = readers[0].GetValue<string>(index);
                var claim = readers[1].GetValue<string>(index);

                var claimValue = ExtractClaimFromJwt(jwt, claim);
                writer.WriteValue(claimValue, index);
            }
        });

        connection.RegisterTableFunction<string>("extract_claims_from_jwt", parameters =>
        {
            var jwt = parameters[0].GetValue<string>();

            return new TableFunction(new List<ColumnInfo>
            {
                new("claim_name", typeof(string)),
                new("claim_value", typeof(string)),
            }, ExtractClaimsFromJwt(jwt));
        }, (item, writers, rowIndex) =>
        {
            var claim = (KeyValuePair<string, string>)item;
            writers[0].WriteValue(claim.Key, rowIndex);
            writers[1].WriteValue(claim.Value, rowIndex);
        });
    }

    private static bool IsJwt(string jwt)
    {
        var jwtHandler = new JwtSecurityTokenHandler();
        return jwtHandler.CanReadToken(jwt);
    }

    private static string ExtractClaimFromJwt(string jwt, string claim)
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