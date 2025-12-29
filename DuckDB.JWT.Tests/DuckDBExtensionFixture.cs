using DuckDB.NET.Data;

namespace DuckDB.JWT.Tests;

public class DuckDBExtensionFixture : IDisposable
{
    private readonly DuckDBConnection connection;
    private readonly string extensionDirectory;
    private bool disposed;

    public DuckDBExtensionFixture()
    {
        // Set up custom extension directory
        extensionDirectory = Path.Combine(AppContext.BaseDirectory, "extensions-install");
        
        try
        {
            if (Directory.Exists(extensionDirectory))
            {
                Directory.Delete(extensionDirectory, recursive: true);
            }
        }
        catch { }

        Directory.CreateDirectory(extensionDirectory);

        // Allow unsigned extensions and set custom extension directory
        var connectionString = $"DataSource=:memory:;allow_unsigned_extensions=true;extension_directory={extensionDirectory}";
        connection = new DuckDBConnection(connectionString);
        connection.Open();

        LoadJwtExtension();
    }

    public DuckDBConnection Connection => connection;

    private void LoadJwtExtension()
    {
        var extensionPath = GetExtensionPath();

        if (!File.Exists(extensionPath))
        {
            throw new FileNotFoundException($"JWT extension not found at: {extensionPath}");
        }

        using var command = connection.CreateCommand();
        command.CommandText = $"INSTALL '{extensionPath}';";
        command.ExecuteNonQuery();

        command.CommandText = "LOAD jwt;";
        command.ExecuteNonQuery();
    }

    private static string GetExtensionPath()
    {
        var baseDirectory = AppContext.BaseDirectory;
        var extensionPath = Path.Combine(baseDirectory, "extensions", "jwt.duckdb_extension");
        return extensionPath;
    }

    public void Dispose()
    {
        if (disposed) return;

        connection?.Dispose();

        try
        {
            if (Directory.Exists(extensionDirectory))
            {
                Directory.Delete(extensionDirectory, recursive: true);
            }
        }
        catch { }

        disposed = true;
        GC.SuppressFinalize(this);
    }
}
