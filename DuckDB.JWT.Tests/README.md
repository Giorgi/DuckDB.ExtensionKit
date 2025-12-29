# DuckDB.JWT.Tests

This test project validates the functionality of the DuckDB JWT extension.

## Overview

The test project:
- Automatically publishes the JWT extension with Native AOT before building
- Loads the compiled extension into an in-memory DuckDB database
- Tests all three JWT functions provided by the extension

## Requirements

- .NET 10.0 SDK
- DuckDB.NET.Data.Full NuGet package
- TUnit test framework

## Running Tests

Before running tests, you need to build the JWT extension:

```bash
# Build the extension for your current platform
pwsh ./build-extension.ps1

# Or on Windows with PowerShell
.\build-extension.ps1

# Then run the tests
dotnet test
```

The `build-extension.ps1` script will:
1. Detect your current platform (Windows/Linux/macOS and x64/ARM64)
2. Publish the DuckDB.JWT extension with Native AOT for your platform
3. Place the compiled extension in the correct location

The test project will automatically copy the extension to its output directory and load it before running tests.

## Test Structure

### DuckDBExtensionFixture
A shared test fixture that manages the DuckDB connection lifecycle and loads the JWT extension.

**Configuration:**
- Uses an in-memory database (`:memory:`)
- Enables unsigned extensions (`allow_unsigned_extensions=true`)
- Sets custom extension directory (`extension_directory`) to `bin/Debug/net10.0/extensions/`
- Loads the JWT extension from the custom directory

This ensures tests run in isolation with their own extension directory, preventing conflicts with system-wide DuckDB installations.

### JwtTokenHelper
Utility class for creating test JWT tokens with various claims.

### Test Classes

1. **IsJwtFunctionTests** - Tests the `is_jwt(token)` function
   - Validates JWT token format
   - Tests valid and invalid tokens
   - Tests batch processing

2. **ExtractClaimFromJwtFunctionTests** - Tests the `extract_claim_from_jwt(token, claim)` function
   - Extracts specific claims from tokens
   - Tests standard and custom claims
   - Tests non-existent claims returning null

3. **ExtractClaimsFromJwtTableFunctionTests** - Tests the `extract_claims_from_jwt(token)` table function
   - Returns all claims as rows
   - Tests filtering, joins, and aggregations
   - Validates SQL operations on claim data

## Extension Functions Tested

### is_jwt(token VARCHAR) ? BOOLEAN
Validates if a string is a valid JWT token.

```sql
SELECT is_jwt('eyJhbGc...') as valid;
```

### extract_claim_from_jwt(token VARCHAR, claim VARCHAR) ? VARCHAR
Extracts a specific claim value from a JWT token.

```sql
SELECT extract_claim_from_jwt('eyJhbGc...', 'name') as name;
```

### extract_claims_from_jwt(token VARCHAR) ? TABLE(claim_name VARCHAR, claim_value VARCHAR)
Returns all claims from a JWT token as a table.

```sql
SELECT * FROM extract_claims_from_jwt('eyJhbGc...');
```

## Platform Support

The tests automatically build the extension for your current platform:
- Windows (x64, ARM64)
- Linux (x64, ARM64)
- macOS (x64, ARM64)

## Notes

- The test project does not directly reference DuckDB.JWT to ensure testing of the compiled extension
- Extension is loaded dynamically using DuckDB's LOAD command
- Tests use an in-memory DuckDB database for isolation
