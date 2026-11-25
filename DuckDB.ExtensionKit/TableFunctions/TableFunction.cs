using System.Collections;

namespace DuckDB.ExtensionKit.TableFunctions;

// These records have the shape of the ones of the same names in DuckDB.NET (DuckDBConnection.TableFunction.cs): the
// registration code linked from DuckDB.NET (external/DuckDB.NET/DuckDB.NET.Data/Functions) uses them by name.

/// <summary>
/// The number of rows a table function returns, for the optimizer of DuckDB.
/// </summary>
/// <param name="Value">The number of rows.</param>
/// <param name="IsExact">
/// <c>false</c>, the default: <paramref name="Value"/> is an estimate, which the optimizer uses for planning only.
/// <c>true</c>: the function returns exactly <paramref name="Value"/> rows, and DuckDB also takes it as the most rows
/// the function can return. A function that returns more rows than it said can get a wrong <c>sum</c>, or a
/// <c>LIMIT</c> that is not applied, so pass <c>true</c> only for a count that is certain.
/// </param>
public record CardinalityHint(ulong Value, bool IsExact = false);

/// <summary>
/// A column that a query reads from a table function: its index among the function's columns, its name and type.
/// </summary>
public record ProjectedColumn(int Index, string Name, Type Type);

public record TableFunction
{
    public IReadOnlyList<ColumnInfo> Columns { get; }
    public CardinalityHint? Cardinality { get; }
    public IEnumerable? Data { get; }
    internal Func<IReadOnlyList<ProjectedColumn>, IEnumerable>? DataFactory { get; }

    public TableFunction(IReadOnlyList<ColumnInfo> columns, IEnumerable data, CardinalityHint? cardinality = null)
    {
        Columns = columns;
        Data = data;
        Cardinality = cardinality;
    }

    internal TableFunction(IReadOnlyList<ColumnInfo> columns, Func<IReadOnlyList<ProjectedColumn>, IEnumerable> dataFactory, CardinalityHint? cardinality = null)
    {
        Columns = columns;
        DataFactory = dataFactory;
        Cardinality = cardinality;
    }
}
