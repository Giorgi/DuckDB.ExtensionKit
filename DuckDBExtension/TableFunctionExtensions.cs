using DuckDB.NET.Data.DataChunk.Writer;
using System.Collections;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DuckDB.Extension;

static class TableFunctionExtensions
{
    public static void RegisterTableFunction(this DuckDBNativeConnection connection, string name, Func<TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, _ => resultCallback(), mapperCallback);

    public static void RegisterTableFunction<T>(this DuckDBNativeConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, resultCallback, mapperCallback, typeof(T));

    public static void RegisterTableFunction<T1, T2>(this DuckDBNativeConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, resultCallback, mapperCallback, typeof(T1), typeof(T2));

    public static void RegisterTableFunction<T1, T2, T3>(this DuckDBNativeConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3));

    public static void RegisterTableFunction<T1, T2, T3, T4>(this DuckDBNativeConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4));

    public static void RegisterTableFunction<T1, T2, T3, T4, T5>(this DuckDBNativeConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5));

    public static void RegisterTableFunction<T1, T2, T3, T4, T5, T6>(this DuckDBNativeConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6));

    public static void RegisterTableFunction<T1, T2, T3, T4, T5, T6, T7>(this DuckDBNativeConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6), typeof(T7));

    public static void RegisterTableFunction<T1, T2, T3, T4, T5, T6, T7, T8>(this DuckDBNativeConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6), typeof(T7), typeof(T8));

    private static unsafe void RegisterTableFunctionInternal(this DuckDBNativeConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback, params Type[] parameterTypes)
    {
        var function = NativeMethods.TableFunction.DuckDBCreateTableFunction();
        fixed (byte* namePtr = System.Text.Encoding.UTF8.GetBytes(name + "\0"))
        {
            NativeMethods.TableFunction.DuckDBTableFunctionSetName(function, namePtr);
        }

        foreach (var type in parameterTypes)
        {
            using var logicalType = type.GetLogicalType();
            NativeMethods.TableFunction.DuckDBTableFunctionAddParameter(function, logicalType);
        }

        var tableFunctionInfo = new TableFunctionInfo(resultCallback, mapperCallback);

        NativeMethods.TableFunction.DuckDBTableFunctionSetBind(function, &Bind);
        NativeMethods.TableFunction.DuckDBTableFunctionSetInit(function, &Init);
        NativeMethods.TableFunction.DuckDBTableFunctionSetFunction(function, &TableFunction);
        NativeMethods.TableFunction.DuckDBTableFunctionSetExtraInfo(function, tableFunctionInfo.ToHandle(), &DestroyExtraInfo);

        var state = NativeMethods.TableFunction.DuckDBRegisterTableFunction(connection.Connection, function);

        if (state != DuckDBState.Success)
        {
            throw new InvalidOperationException($"Error registering user defined table function: {name}");
        }

        NativeMethods.TableFunction.DuckDBDestroyTableFunction(function);
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    public static unsafe void Bind(IntPtr info)
    {
        IDuckDBValueReader[] parameters = [];
        try
        {
            var handle = GCHandle.FromIntPtr(new IntPtr(NativeMethods.TableFunction.DuckDBBindGetExtraInfo(info)));

            if (handle.Target is not TableFunctionInfo functionInfo)
            {
                throw new InvalidOperationException("User defined table function bind failed. Bind extra info is null");
            }

            parameters = new IDuckDBValueReader[NativeMethods.TableFunction.DuckDBBindGetParameterCount(info)];

            for (var i = 0; i < parameters.Length; i++)
            {
                var value = NativeMethods.TableFunction.DuckDBBindGetParameter(info, (ulong)i);
                parameters[i] = new DuckDBValue(value);
            }

            var tableFunctionData = functionInfo.Bind(parameters);

            foreach (var columnInfo in tableFunctionData.Columns)
            {
                using var logicalType = columnInfo.Type.GetLogicalType();
                fixed (byte* columnNamePtr = System.Text.Encoding.UTF8.GetBytes(columnInfo.Name + "\0"))
                {
                    NativeMethods.TableFunction.DuckDBBindAddResultColumn(info, columnNamePtr, logicalType);
                }
            }

            var bindData = new TableFunctionBindData(tableFunctionData.Columns, tableFunctionData.Data.GetEnumerator());

            NativeMethods.TableFunction.DuckDBBindSetBindData(info, bindData.ToHandle(), &DestroyExtraInfo);
        }
        catch (Exception ex)
        {
            fixed (byte* errorPtr = System.Text.Encoding.UTF8.GetBytes(ex.Message + "\0"))
            {
                NativeMethods.TableFunction.DuckDBBindSetError(info, errorPtr);
            }
        }
        finally
        {
            foreach (var parameter in parameters)
            {
                (parameter as IDisposable)?.Dispose();
            }
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    public static void Init(IntPtr info) { }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    public static unsafe void TableFunction(IntPtr info, IntPtr chunk)
    {
        try
        {
            var bindData = GCHandle.FromIntPtr(new IntPtr(NativeMethods.TableFunction.DuckDBFunctionGetBindData(info)));
            var extraInfo = GCHandle.FromIntPtr(new IntPtr(NativeMethods.TableFunction.DuckDBFunctionGetExtraInfo(info)));

            if (bindData.Target is not TableFunctionBindData tableFunctionBindData)
            {
                throw new InvalidOperationException("User defined table function failed. Function bind data is null");
            }

            if (extraInfo.Target is not TableFunctionInfo tableFunctionInfo)
            {
                throw new InvalidOperationException("User defined table function failed. Function extra info is null");
            }

            var dataChunk = new DuckDBDataChunk(chunk);

            var writers = new VectorDataWriterBase[tableFunctionBindData.Columns.Count];
            for (var columnIndex = 0; columnIndex < tableFunctionBindData.Columns.Count; columnIndex++)
            {
                var column = tableFunctionBindData.Columns[columnIndex];
                var vector = NativeMethods.DataChunks.DuckDBDataChunkGetVector(dataChunk, columnIndex);

                using var logicalType = column.Type.GetLogicalType();
                writers[columnIndex] = VectorDataWriterFactory.CreateWriter(vector, logicalType);
            }

            ulong size = 0;

            for (; size < DuckDBGlobalData.VectorSize; size++)
            {
                if (tableFunctionBindData.DataEnumerator.MoveNext())
                {
                    tableFunctionInfo.Mapper(tableFunctionBindData.DataEnumerator.Current, writers, size);
                }
                else
                {
                    break;
                }
            }

            NativeMethods.DataChunks.DuckDBDataChunkSetSize(dataChunk, size);
        }
        catch (Exception ex)
        {
            fixed (byte* errorPtr = System.Text.Encoding.UTF8.GetBytes(ex.Message + "\0"))
            {
                NativeMethods.TableFunction.DuckDBFunctionSetError(info, errorPtr);
            }
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void DestroyExtraInfo(void* pointer) => new IntPtr(pointer).FreeHandle();
}

public record ColumnInfo(string Name, Type Type);

public record TableFunction(IReadOnlyList<ColumnInfo> Columns, IEnumerable Data);

class TableFunctionInfo(Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> bind, Action<object?, VectorDataWriterBase[], ulong> mapper)
{
    public Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> Bind { get; } = bind;
    public Action<object?, VectorDataWriterBase[], ulong> Mapper { get; } = mapper;
}

class TableFunctionBindData(IReadOnlyList<ColumnInfo> columns, IEnumerator dataEnumerator) : IDisposable
{
    public IReadOnlyList<ColumnInfo> Columns { get; } = columns;
    public IEnumerator DataEnumerator { get; } = dataEnumerator;

    public void Dispose()
    {
        (DataEnumerator as IDisposable)?.Dispose();
    }
}

public interface IDuckDBValueReader
{
    bool IsNull();
    T GetValue<T>();
}