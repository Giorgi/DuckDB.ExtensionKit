using DuckDB.NET.Data;

namespace DuckDB.JWT.Tests;

public class DuckDBExtensionFixture : IDisposable
{
    private readonly string extensionDirectory;

    public DuckDBExtensionFixture()
    {
        extensionDirectory = Path.Combine(AppContext.BaseDirectory, "extensions-install");

        Directory.CreateDirectory(extensionDirectory);

        // Install extension once using a temporary connection
        using var connection = CreateConnectionCore();
        InstallExtension(connection);
    }

    public DuckDBConnection CreateConnection()
    {
        var connection = CreateConnectionCore();

        using var command = connection.CreateCommand();
        command.CommandText = "LOAD jwt;";
        command.ExecuteNonQuery();

        return connection;
    }

    private DuckDBConnection CreateConnectionCore()
    {
        var connectionString = $"DataSource=:memory:;allow_unsigned_extensions=true;extension_directory={extensionDirectory}";
        var connection = new DuckDBConnection(connectionString);
        connection.Open();
        return connection;
    }

    private static void InstallExtension(DuckDBConnection connection)
    {
        var extensionPath = Path.Combine(AppContext.BaseDirectory, "extensions", "jwt.duckdb_extension");

        if (!File.Exists(extensionPath))
        {
            throw new FileNotFoundException($"JWT extension not found at: {extensionPath}");
        }

        using var command = connection.CreateCommand();
        command.CommandText = $"INSTALL '{extensionPath}';";
        command.ExecuteNonQuery();
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(extensionDirectory, true);
        }
        catch (Exception) { }
    }
}
