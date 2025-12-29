using DuckDB.NET.Data;

namespace DuckDB.JWT.Tests;

public class DuckDBExtensionFixture : IDisposable
{
    private bool disposed;

    public DuckDBExtensionFixture()
    {
        var extensionDirectory = Path.Combine(AppContext.BaseDirectory, "extensions-install");

        Directory.CreateDirectory(extensionDirectory);

        // Allow unsigned extensions and set custom extension directory
        var connectionString = $"DataSource=:memory:;allow_unsigned_extensions=true;extension_directory={extensionDirectory}";
        Connection = new DuckDBConnection(connectionString);
        Connection.Open();

        LoadJwtExtension();
    }

    public DuckDBConnection Connection { get; }

    private void LoadJwtExtension()
    {
        var extensionPath = Path.Combine(AppContext.BaseDirectory, "extensions", "jwt.duckdb_extension");

        if (!File.Exists(extensionPath))
        {
            throw new FileNotFoundException($"JWT extension not found at: {extensionPath}");
        }

        using var command = Connection.CreateCommand();
        command.CommandText = $"INSTALL '{extensionPath}';";
        command.ExecuteNonQuery();

        command.CommandText = "LOAD jwt;";
        command.ExecuteNonQuery();
    }

    public void Dispose()
    {
        if (disposed) return;

        Connection?.Dispose();

        disposed = true;
        GC.SuppressFinalize(this);
    }
}
