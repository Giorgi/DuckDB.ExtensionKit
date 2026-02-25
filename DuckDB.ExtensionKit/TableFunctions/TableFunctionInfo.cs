using DuckDB.ExtensionKit.DataChunk.Writer;
using DuckDB.ExtensionKit.Native;

namespace DuckDB.ExtensionKit.TableFunctions;

class TableFunctionInfo(Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> bind, Action<object?, VectorDataWriterBase[], ulong> mapper, string[] namedParameterNames)
{
    public Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> Bind { get; } = bind;
    public Action<object?, VectorDataWriterBase[], ulong> Mapper { get; } = mapper;
    public string[] NamedParameterNames { get; } = namedParameterNames;
}

record NamedParameterDefinition(string Name, Type Type);