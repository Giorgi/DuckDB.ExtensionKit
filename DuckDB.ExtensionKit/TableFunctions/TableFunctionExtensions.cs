using DuckDB.ExtensionKit.Common;
using DuckDB.ExtensionKit.DataChunk.Writer;
using DuckDB.ExtensionKit.Extensions;
using DuckDB.ExtensionKit.Native;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace DuckDB.ExtensionKit.TableFunctions;

public static class TableFunctionExtensions
{
    public static void RegisterTableFunction(this DuckDBConnection connection, string name, Func<TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, _ => resultCallback(), mapperCallback);

    public static void RegisterTableFunction<T>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T));

    public static void RegisterTableFunction<T1, T2>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2));

    public static void RegisterTableFunction<T1, T2, T3>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3));

    public static void RegisterTableFunction<T1, T2, T3, T4>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4));

    public static void RegisterTableFunction<T1, T2, T3, T4, T5>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5));

    public static void RegisterTableFunction<T1, T2, T3, T4, T5, T6>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6));

    public static void RegisterTableFunction<T1, T2, T3, T4, T5, T6, T7>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6), typeof(T7));

    public static void RegisterTableFunction<T1, T2, T3, T4, T5, T6, T7, T8>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) => connection.RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6), typeof(T7), typeof(T8));

    private static void RegisterTableFunctionInternal(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback, params Type[] parameterTypes)
    {
        var logicalTypes = Array.ConvertAll(parameterTypes, TypeExtensions.GetLogicalType);
        connection.RegisterTableFunctionInternal(name, (positional, _) => resultCallback(positional), mapperCallback, logicalTypes, []);
    }

    internal static unsafe void RegisterTableFunctionInternal(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback, DuckDBLogicalType[] positionalLogicalTypes, NamedParameterDefinition[] namedParameters)
    {
        var function = NativeMethods.NativeMethods.TableFunction.DuckDBCreateTableFunction();
        fixed (byte* namePtr = Encoding.UTF8.GetBytes(name + "\0"))
        {
            NativeMethods.NativeMethods.TableFunction.DuckDBTableFunctionSetName(function, namePtr);
        }

        foreach (var logicalType in positionalLogicalTypes)
        {
            NativeMethods.NativeMethods.TableFunction.DuckDBTableFunctionAddParameter(function, logicalType);
            logicalType.Dispose();
        }

        foreach (var param in namedParameters)
        {
            using var logicalType = param.Type.GetLogicalType();
            fixed (byte* paramNamePtr = Encoding.UTF8.GetBytes(param.Name + "\0"))
            {
                NativeMethods.NativeMethods.TableFunction.DuckDBTableFunctionAddNamedParameter(function, paramNamePtr, logicalType);
            }
        }

        var tableFunctionInfo = new TableFunctionInfo(resultCallback, mapperCallback, Array.ConvertAll(namedParameters, p => p.Name));

        NativeMethods.NativeMethods.TableFunction.DuckDBTableFunctionSetBind(function, &Bind);
        NativeMethods.NativeMethods.TableFunction.DuckDBTableFunctionSetInit(function, &Init);
        NativeMethods.NativeMethods.TableFunction.DuckDBTableFunctionSetFunction(function, &TableFunction);
        NativeMethods.NativeMethods.TableFunction.DuckDBTableFunctionSetExtraInfo(function, tableFunctionInfo.ToHandle(), &DestroyExtraInfo);

        var state = NativeMethods.NativeMethods.TableFunction.DuckDBRegisterTableFunction(connection.Connection, function);

        if (state != DuckDBState.Success)
        {
            throw new InvalidOperationException($"Error registering user defined table function: {name}");
        }

        NativeMethods.NativeMethods.TableFunction.DuckDBDestroyTableFunction(function);
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void Bind(IntPtr info)
    {
        IDuckDBValueReader[] parameters = [];
        Dictionary<string, IDuckDBValueReader> named = [];
        try
        {
            var handle = GCHandle.FromIntPtr(new IntPtr(NativeMethods.NativeMethods.TableFunction.DuckDBBindGetExtraInfo(info)));

            if (handle.Target is not TableFunctionInfo functionInfo)
            {
                throw new InvalidOperationException("User defined table function bind failed. Bind extra info is null");
            }

            parameters = new IDuckDBValueReader[NativeMethods.NativeMethods.TableFunction.DuckDBBindGetParameterCount(info)];

            for (var i = 0; i < parameters.Length; i++)
            {
                var value = NativeMethods.NativeMethods.TableFunction.DuckDBBindGetParameter(info, (ulong)i);
                parameters[i] = new DuckDBValue(value);
            }

            // When a named parameter is omitted in SQL, duckdb_bind_get_named_parameter returns a null pointer.
            // We substitute it with NullValueReader so CompileValueReader's IsNull() check
            // correctly handles it (returns default for nullable, throws for non-nullable).
            foreach (var paramName in functionInfo.NamedParameterNames)
            {
                fixed (byte* paramNamePtr = Encoding.UTF8.GetBytes(paramName + "\0"))
                {
                    var value = NativeMethods.NativeMethods.TableFunction.DuckDBBindGetNamedParameter(info, paramNamePtr);
                    named[paramName] = value == IntPtr.Zero ? NullValueReader.Instance : new DuckDBValue(value);
                }
            }

            var tableFunctionData = functionInfo.Bind(parameters, named);

            foreach (var columnInfo in tableFunctionData.Columns)
            {
                using var logicalType = columnInfo.Type.GetLogicalType();
                fixed (byte* columnNamePtr = Encoding.UTF8.GetBytes(columnInfo.Name + "\0"))
                {
                    NativeMethods.NativeMethods.TableFunction.DuckDBBindAddResultColumn(info, columnNamePtr, logicalType);
                }
            }

            var bindData = new TableFunctionBindData(tableFunctionData.Columns, tableFunctionData.Data.GetEnumerator());

            NativeMethods.NativeMethods.TableFunction.DuckDBBindSetBindData(info, bindData.ToHandle(), &DestroyExtraInfo);
        }
        catch (Exception ex)
        {
            fixed (byte* errorPtr = Encoding.UTF8.GetBytes(ex.Message + "\0"))
            {
                NativeMethods.NativeMethods.TableFunction.DuckDBBindSetError(info, errorPtr);
            }
        }
        finally
        {
            foreach (var parameter in parameters)
            {
                (parameter as IDisposable)?.Dispose();
            }

            foreach (var namedParam in named.Values)
            {
                (namedParam as IDisposable)?.Dispose();
            }
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void Init(IntPtr info) { }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void TableFunction(IntPtr info, IntPtr chunk)
    {
        VectorDataWriterBase[] writers = [];
        DuckDBLogicalType[] logicalTypes = [];
        try
        {
            var bindData = GCHandle.FromIntPtr(new IntPtr(NativeMethods.NativeMethods.TableFunction.DuckDBFunctionGetBindData(info)));
            var extraInfo = GCHandle.FromIntPtr(new IntPtr(NativeMethods.NativeMethods.TableFunction.DuckDBFunctionGetExtraInfo(info)));

            if (bindData.Target is not TableFunctionBindData tableFunctionBindData)
            {
                throw new InvalidOperationException("User defined table function failed. Function bind data is null");
            }

            if (extraInfo.Target is not TableFunctionInfo tableFunctionInfo)
            {
                throw new InvalidOperationException("User defined table function failed. Function extra info is null");
            }

            var dataChunk = new DuckDBDataChunk(chunk);

            writers = new VectorDataWriterBase[tableFunctionBindData.Columns.Count];
            logicalTypes = new DuckDBLogicalType[tableFunctionBindData.Columns.Count];

            for (var columnIndex = 0; columnIndex < tableFunctionBindData.Columns.Count; columnIndex++)
            {
                var column = tableFunctionBindData.Columns[columnIndex];
                var vector = NativeMethods.NativeMethods.DataChunks.DuckDBDataChunkGetVector(dataChunk, columnIndex);

                logicalTypes[columnIndex] = column.Type.GetLogicalType();
                writers[columnIndex] = VectorDataWriterFactory.CreateWriter(vector, logicalTypes[columnIndex]);
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

            NativeMethods.NativeMethods.DataChunks.DuckDBDataChunkSetSize(dataChunk, size);
        }
        catch (Exception ex)
        {
            fixed (byte* errorPtr = Encoding.UTF8.GetBytes(ex.Message + "\0"))
            {
                NativeMethods.NativeMethods.TableFunction.DuckDBFunctionSetError(info, errorPtr);
            }
        }
        finally
        {
            foreach (var writer in writers)
            {
                writer?.Dispose();
            }

            foreach (var logicalType in logicalTypes)
            {
                logicalType?.Dispose();
            }
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void DestroyExtraInfo(void* pointer) => new IntPtr(pointer).FreeHandle();

    private class NullValueReader : IDuckDBValueReader
    {
        public static readonly NullValueReader Instance = new();
        public bool IsNull() => true;
        public T GetValue<T>() => throw new InvalidOperationException("Cannot read value from a null parameter.");
    }
}