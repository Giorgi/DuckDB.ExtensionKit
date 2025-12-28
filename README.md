# DuckDB Extensions in C#

Build native DuckDB extensions using C# and [.NET AOT compilation](https://learn.microsoft.com/en-us/dotnet/core/deploying/native-aot/).

## Getting Started

Clone with submodules to include the required extension packaging script:

```bash
git clone --recurse-submodules https://github.com/Giorgi/DuckDB.ExtensionKit.git
```

Or if already cloned:

```bash
git submodule update --init --recursive
```

## Projects

| Project | Description |
|---------|-------------|
| **DuckDB.ExtensionKit** | Core runtime library with DuckDB C API bindings, type-safe function registration, and vector data readers/writers |
| **DuckDB.ExtensionKit.Generators** | Source generator that auto-generates the native entry point boilerplate |
| **DuckDB.JWT** | Example extension implementing JWT functions (validates tokens, extracts claims) |

## Building an Extension

### 1. Create a project

Reference the toolkit packages and configure your extension name:

```xml
<PropertyGroup>
  <ExtensionName>myextension</ExtensionName>
  <DuckDBVersion>v1.2.0</DuckDBVersion>
  <PublishAot>true</PublishAot>
</PropertyGroup>

<ItemGroup>
  <ProjectReference Include="..\DuckDB.ExtensionKit\DuckDB.ExtensionKit.csproj" />
  <ProjectReference Include="..\DuckDB.ExtensionKit.Generators\DuckDB.ExtensionKit.Generators.csproj" OutputItemType="Analyzer" ReferenceOutputAssembly="false" />
</ItemGroup>
```

### 2. Define your extension

Create a partial class with the `[DuckDBExtension]` attribute and implement `RegisterFunctions`:

```csharp
[DuckDBExtension]
public static partial class MyExtension
{
    private static void RegisterFunctions(DuckDBConnection connection)
    {
        // Register a scalar function
        connection.RegisterScalarFunction<string, int>("string_length",
            (readers, writer, rowCount) =>
            {
                for (ulong i = 0; i < rowCount; i++)
                {
                    var value = readers[0].GetValue<string>(i);
                    writer.WriteValue(value?.Length ?? 0, i);
                }
            });
    }
}
```

The source generator automatically creates the native entry point (`myextension_init_c_api`).

See the **DuckDB.JWT** project in this repo for a complete example with scalar and table functions.

### 3. Build and publish

```bash
dotnet publish -c Release -r win-x64   # or linux-x64, osx-arm64, etc.
```

This also runs a post-publish Python script (`append_extension_metadata.py`) that appends DuckDB extension metadata to the native binary. This metadata is required for DuckDB to recognize and load the file as a valid extension.

The output is a `.duckdb_extension` file ready to load into DuckDB.

## Loading and Testing

Since community extensions are unsigned, start DuckDB with the `-unsigned` flag (see [Unsigned Extensions](https://duckdb.org/docs/stable/extensions/extension_distribution#unsigned-extensions)):

```bash
duckdb -unsigned
```

Then install and load your extension:

```sql
-- Install and load the extension
INSTALL 'path/to/jwt.duckdb_extension';
LOAD jwt;

-- Test scalar functions
SELECT is_jwt('eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6ImExZmIyY2NjN2FiMjBiMDYyNzJmNGUxMjIwZDEwZmZlIn0.eyJpc3MiOiJodHRwczovL2lkcC5sb2NhbCIsImF1ZCI6Im15X2NsaWVudF9hcHAiLCJuYW1lIjoiR2lvcmdpIERhbGFraXNodmlsaSIsInN1YiI6IjViZTg2MzU5MDczYzQzNGJhZDJkYTM5MzIyMjJkYWJlIiwiYWRtaW4iOnRydWUsImV4cCI6MTc2NjU5MTI2NywiaWF0IjoxNzY2NTkwOTY3fQ.N7h2xc4rgS4oPo8IO9wyG1lnr2wqTUC80YudWTXp7rXmU2JdsUiweKmuYVVbygdJAR4PJmbQtak4_VuZg2fZFILVpzDyLvGITfUW_18XuDQ_SIm3VlfAuHOVHfruuvvSAfjUkTW2Jlrv3ihFYgusV58vjhcVFHssOGMEbtMNo10Jf62dczVVGNZXh_OOLS0nTLffhY94sZddqQIE56W8xhLK5YMO4gO8voMzhUwDwucnVvyNfui38MPDNdTSKjn3Ab0hG8jzOVhbYSCHf0eQsbxPzGtXUCJobScWDb78IphFWec6W4ugIYp5CMh3C_noQi94NYjQg2P-AJ5FLCKzKA');
-- Returns: true

SELECT extract_claim_from_jwt('eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6ImExZmIyY2NjN2FiMjBiMDYyNzJmNGUxMjIwZDEwZmZlIn0.eyJpc3MiOiJodHRwczovL2lkcC5sb2NhbCIsImF1ZCI6Im15X2NsaWVudF9hcHAiLCJuYW1lIjoiR2lvcmdpIERhbGFraXNodmlsaSIsInN1YiI6IjViZTg2MzU5MDczYzQzNGJhZDJkYTM5MzIyMjJkYWJlIiwiYWRtaW4iOnRydWUsImV4cCI6MTc2NjU5MTI2NywiaWF0IjoxNzY2NTkwOTY3fQ.N7h2xc4rgS4oPo8IO9wyG1lnr2wqTUC80YudWTXp7rXmU2JdsUiweKmuYVVbygdJAR4PJmbQtak4_VuZg2fZFILVpzDyLvGITfUW_18XuDQ_SIm3VlfAuHOVHfruuvvSAfjUkTW2Jlrv3ihFYgusV58vjhcVFHssOGMEbtMNo10Jf62dczVVGNZXh_OOLS0nTLffhY94sZddqQIE56W8xhLK5YMO4gO8voMzhUwDwucnVvyNfui38MPDNdTSKjn3Ab0hG8jzOVhbYSCHf0eQsbxPzGtXUCJobScWDb78IphFWec6W4ugIYp5CMh3C_noQi94NYjQg2P-AJ5FLCKzKA', 'name');
-- Returns: Giorgi Dalakishvili

-- Test table function
SELECT * FROM extract_claims_from_jwt('eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6ImExZmIyY2NjN2FiMjBiMDYyNzJmNGUxMjIwZDEwZmZlIn0.eyJpc3MiOiJodHRwczovL2lkcC5sb2NhbCIsImF1ZCI6Im15X2NsaWVudF9hcHAiLCJuYW1lIjoiR2lvcmdpIERhbGFraXNodmlsaSIsInN1YiI6IjViZTg2MzU5MDczYzQzNGJhZDJkYTM5MzIyMjJkYWJlIiwiYWRtaW4iOnRydWUsImV4cCI6MTc2NjU5MTI2NywiaWF0IjoxNzY2NTkwOTY3fQ.N7h2xc4rgS4oPo8IO9wyG1lnr2wqTUC80YudWTXp7rXmU2JdsUiweKmuYVVbygdJAR4PJmbQtak4_VuZg2fZFILVpzDyLvGITfUW_18XuDQ_SIm3VlfAuHOVHfruuvvSAfjUkTW2Jlrv3ihFYgusV58vjhcVFHssOGMEbtMNo10Jf62dczVVGNZXh_OOLS0nTLffhY94sZddqQIE56W8xhLK5YMO4gO8voMzhUwDwucnVvyNfui38MPDNdTSKjn3Ab0hG8jzOVhbYSCHf0eQsbxPzGtXUCJobScWDb78IphFWec6W4ugIYp5CMh3C_noQi94NYjQg2P-AJ5FLCKzKA');
```

| claim_name | claim_value                      |
|------------|----------------------------------|
| iss        | https://idp.local                |
| aud        | my_client_app                    |
| name       | Giorgi Dalakishvili              |
| sub        | 5be86359073c434bad2da3932222dabe |
| admin      | true                             |
| exp        | 1766591267                       |
| iat        | 1766590967                       |

## Unstable API

To use DuckDB's [unstable Extension C API functions](https://github.com/duckdb/extension-template-c#using-unstable-extension-c-api-functionality), set `UseUnstableApi` in your `.csproj`:

```xml
<PropertyGroup>
  <UseUnstableApi>true</UseUnstableApi>
</PropertyGroup>
```

This changes the ABI type to `C_STRUCT_UNSTABLE` and suppresses the experimental warnings on unstable API functions. Note that using the unstable API pins your extension to the exact DuckDB version.

## Features

- **Type-safe APIs** - Register scalar and table functions with generic type parameters
- **Automatic marshalling** - Vector readers/writers handle DuckDB's columnar format
- **AOT compilation** - Produces standalone native binaries with no .NET runtime dependency
- **Cross-platform** - Build for Windows, Linux, and macOS (x64 and ARM64)
