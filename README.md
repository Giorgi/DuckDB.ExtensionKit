# DuckDB Extensions in C#

Build native DuckDB extensions using C# and [.NET AOT compilation](https://learn.microsoft.com/en-us/dotnet/core/deploying/native-aot/).

## Getting Started

Clone with submodules. The build needs both: `external/DuckDB.NET` holds the vector readers and writers that
ExtensionKit compiles (see [Code Shared with DuckDB.NET](#code-shared-with-duckdbnet)), and
`DuckDB.JWT/extension-ci-tools` holds the extension packaging script.

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
            value => value?.Length ?? 0);

        // Register a table function with expression-based projection
        connection.RegisterTableFunction("get_items",
            (string category) => GetItems(category),
            (Item item) => new { name = item.Name, price = item.Price });

        // Table function with named parameters
        // SQL: SELECT * FROM get_items('toys', max_rows := 10)
        connection.RegisterTableFunction("get_items_filtered",
            (string category, [Named("max_rows")] int? limit) =>
                GetItems(category).Take(limit ?? int.MaxValue),
            (Item item) => new { name = item.Name, price = item.Price });
    }
}
```

Named parameters use the `[Named]` attribute on lambda parameters. By default the SQL parameter name matches the C# name; use `[Named("custom_name")]` to override it. Named parameters are optional - omitted ones receive `null`.

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

-- Lifetime of a token, from its nbf and exp claims (TIMESTAMPTZ, NULL when the claim is missing).
-- jwt_is_valid_at is true from nbf, inclusive, until exp, exclusive. The signature is not checked.
SELECT jwt_valid_from(token), jwt_valid_to(token), jwt_is_valid_at(token, now()) FROM tokens;

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

## How It Works

1. **Source Generator** - At compile time, the generator finds your `[DuckDBExtension]` class and generates a native entry point function (`{extension}_init_c_api`) marked with `[UnmanagedCallersOnly]`

2. **AOT Compilation** - .NET compiles your code to a native binary that exports the entry point, with no runtime dependency

3. **Extension Loading** - When DuckDB loads your extension, it calls the entry point which:
   - Initializes the C API and receives function pointers to DuckDB's internal APIs
   - Obtains a database connection and calls your `RegisterFunctions` method to register scalar/table functions

## Supported Types

The C# types of a function's parameters and result, and of the columns of a table function, map to DuckDB types:

| C# type | DuckDB type | Parameter | Result / column |
|---------|-------------|:---------:|:---------------:|
| `bool` | BOOLEAN | ✓ | ✓ |
| `sbyte`, `short`, `int`, `long` | TINYINT, SMALLINT, INTEGER, BIGINT | ✓ | ✓ |
| `byte`, `ushort`, `uint`, `ulong` | UTINYINT, USMALLINT, UINTEGER, UBIGINT | ✓ | ✓ |
| `float`, `double` | FLOAT, DOUBLE | ✓ | ✓ |
| `decimal` | DECIMAL(38,18) | ✓ | ✓ |
| `Int128`, `BigInteger` | HUGEINT | ✓ | ✓ |
| `UInt128` | UHUGEINT | ✓ | ✓ |
| `string` | VARCHAR | ✓ | ✓ |
| `Guid` | UUID | ✓ | ✓ |
| `DateOnly`, `DuckDBDateOnly` | DATE | ✓ | ✓ |
| `TimeOnly`, `DuckDBTimeOnly` | TIME | ✓ | ✓ |
| `DateTime` | TIMESTAMP | ✓ | ✓ |
| `DateTimeOffset` | TIMESTAMPTZ | ✓ | ✓ |
| `TimeSpan` | INTERVAL | ✓ | ✓ |
| `List<T>` of a type above | LIST | ✓ | ✓ |
| `byte[]` | BLOB | ✓ | ✓ |
| `object` | any type | ✓ | |

A nullable value type (`int?`, `DateTime?`, ...) or a nullable reference type (`string?`) accepts and returns NULL.
The same goes for list elements: a list with NULL elements needs a nullable element type, `List<int?>` and not
`List<int>`.
DuckDB casts an argument of another type when it can, for example a `DECIMAL(9,2)` to `DECIMAL(38,18)` or a
`TIMESTAMP` to `TIMESTAMPTZ`.

An `object` parameter accepts any type. Its value is the .NET type of the column:

| DuckDB type | .NET value |
|-------------|------------|
| TIMESTAMP, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS, TIMESTAMPTZ | `DateTime` |
| TIMETZ | `DateTimeOffset` |
| HUGEINT, UHUGEINT | `Int128`, `UInt128` |
| BLOB | `byte[]` |
| ENUM | `string` |
| LIST, ARRAY | `List<T>` |
| MAP | `Dictionary<TKey, TValue>` |
| the other types | as in the table above |

Not supported yet in a Native AOT extension: reading a STRUCT or a nested LIST. An INTERVAL with months cannot be
read as `TimeSpan`, nor an infinite DATE or TIMESTAMP as a .NET date.

## Code Shared with DuckDB.NET

The vector readers and writers, their helpers, and the native value types (`DuckDBType`, `DuckDBDateOnly`,
`DuckDBHugeInt`, ...) are not copied into this repository. They are compiled from
[DuckDB.NET](https://github.com/Giorgi/DuckDB.NET), which is the `external/DuckDB.NET` submodule. The linked files are
listed in `DuckDB.ExtensionKit/DuckDB.ExtensionKit.csproj`.

Those files reach native code only through `NativeMethods.*` and the handle types such as `DuckDBLogicalType`. DuckDB.NET
implements them with P/Invoke; this project implements them over the function table that DuckDB passes to an extension
(`NativeMethods/` and `Native/`). A wrapper that a linked file calls must have the signature DuckDB.NET.Bindings gives it,
and a mismatch shows up as a build error here.

To take newer DuckDB.NET code, move the submodule and build:

```bash
git -C external/DuckDB.NET fetch origin develop
git -C external/DuckDB.NET checkout FETCH_HEAD
dotnet build
```

Because of this, the shared types keep DuckDB.NET's namespaces: `DuckDBType` and the other native types are in
`DuckDB.NET.Native`, and `IDuckDBDataReader` / `IDuckDBDataWriter` are in `DuckDB.NET.Data.DataChunk.Reader` /
`DuckDB.NET.Data.DataChunk.Writer`.

## Features

- **Type-safe APIs** - Register scalar and table functions with generic type parameters
- **Automatic marshalling** - Vector readers/writers handle DuckDB's columnar format
- **AOT compilation** - Produces standalone native binaries with no .NET runtime dependency
- **Cross-platform** - Build for Windows, Linux, and macOS (x64 and ARM64)
