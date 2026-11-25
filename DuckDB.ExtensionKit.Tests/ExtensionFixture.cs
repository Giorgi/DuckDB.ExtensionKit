using DuckDB.NET.Data;

namespace DuckDB.ExtensionKit.Tests;

// Installs the test extension (DuckDB.ExtensionKit.TestExtension, published with Native AOT) once, and gives each
// test class a connection with it loaded. Publish the extension before running the tests:
// dotnet publish DuckDB.ExtensionKit.TestExtension -c Release -r <rid>
public class ExtensionFixture : IDisposable
{
    private readonly string extensionDirectory;

    public ExtensionFixture()
    {
        extensionDirectory = Path.Combine(AppContext.BaseDirectory, "extensions-install");
        Directory.CreateDirectory(extensionDirectory);

        var extensionPath = Path.Combine(AppContext.BaseDirectory, "extensions", "ektest.duckdb_extension");

        if (!File.Exists(extensionPath))
        {
            throw new FileNotFoundException($"Test extension not found at: {extensionPath}. Publish DuckDB.ExtensionKit.TestExtension first.");
        }

        using var connection = CreateConnectionCore();
        using var command = connection.CreateCommand();
        command.CommandText = $"INSTALL '{extensionPath}';";
        command.ExecuteNonQuery();
    }

    public DuckDBConnection CreateConnection()
    {
        var connection = CreateConnectionCore();

        using var command = connection.CreateCommand();
        command.CommandText = "LOAD ektest;";
        command.ExecuteNonQuery();

        return connection;
    }

    private DuckDBConnection CreateConnectionCore()
    {
        var connection = new DuckDBConnection($"DataSource=:memory:;allow_unsigned_extensions=true;extension_directory={extensionDirectory}");
        connection.Open();
        return connection;
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

// The base of the test classes: one connection per class, and the results compared in SQL, so that they do not
// depend on how the DuckDB.NET client converts a value.
public abstract class ExtensionTestBase(ExtensionFixture fixture) : IDisposable
{
    protected DuckDBConnection Connection { get; } = fixture.CreateConnection();

    protected object? Scalar(string sql)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar();
    }

    protected List<object?[]> Rows(string sql)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();

        var rows = new List<object?[]>();
        while (reader.Read())
        {
            var row = new object?[reader.FieldCount];
            for (var i = 0; i < row.Length; i++)
            {
                row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }
            rows.Add(row);
        }

        return rows;
    }

    public void Dispose() => Connection.Dispose();
}
