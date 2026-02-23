using DuckDB.ExtensionKit.Common;
using DuckDB.ExtensionKit.DataChunk.Writer;
using DuckDB.ExtensionKit.Extensions;
using DuckDB.ExtensionKit.Native;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DuckDB.ExtensionKit.TableFunctions;

public static class TableFunctionExtensions
{
    public static void RegisterTableFunction(this DuckDBConnection connection, string name, Func<TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, _ => resultCallback(), mapperCallback);

    public static void RegisterTableFunction<T>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, resultCallback, mapperCallback, typeof(T));

    public static void RegisterTableFunction<T1, T2>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, resultCallback, mapperCallback, typeof(T1), typeof(T2));

    public static void RegisterTableFunction<T1, T2, T3>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3));

    public static void RegisterTableFunction<T1, T2, T3, T4>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4));

    public static void RegisterTableFunction<T1, T2, T3, T4, T5>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5));

    public static void RegisterTableFunction<T1, T2, T3, T4, T5, T6>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6));

    public static void RegisterTableFunction<T1, T2, T3, T4, T5, T6, T7>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6), typeof(T7));

    public static void RegisterTableFunction<T1, T2, T3, T4, T5, T6, T7, T8>(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback) =>
        RegisterTableFunctionInternal(connection, name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6), typeof(T7), typeof(T8));

    private static unsafe void RegisterTableFunctionInternal(this DuckDBConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback, params Type[] parameterTypes)
    {
        var function = NativeMethods.NativeMethods.TableFunction.DuckDBCreateTableFunction();
        fixed (byte* namePtr = System.Text.Encoding.UTF8.GetBytes(name + "\0"))
        {
            NativeMethods.NativeMethods.TableFunction.DuckDBTableFunctionSetName(function, namePtr);
        }

        foreach (var type in parameterTypes)
        {
            using var logicalType = type.GetLogicalType();
            NativeMethods.NativeMethods.TableFunction.DuckDBTableFunctionAddParameter(function, logicalType);
        }

        var tableFunctionInfo = new TableFunctionInfo(resultCallback, mapperCallback);

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

            var tableFunctionData = functionInfo.Bind(parameters);

            foreach (var columnInfo in tableFunctionData.Columns)
            {
                using var logicalType = columnInfo.Type.GetLogicalType();
                fixed (byte* columnNamePtr = System.Text.Encoding.UTF8.GetBytes(columnInfo.Name + "\0"))
                {
                    NativeMethods.NativeMethods.TableFunction.DuckDBBindAddResultColumn(info, columnNamePtr, logicalType);
                }
            }

            var bindData = new TableFunctionBindData(tableFunctionData.Columns, tableFunctionData.Data.GetEnumerator());

            NativeMethods.NativeMethods.TableFunction.DuckDBBindSetBindData(info, bindData.ToHandle(), &DestroyExtraInfo);
        }
        catch (Exception ex)
        {
            fixed (byte* errorPtr = System.Text.Encoding.UTF8.GetBytes(ex.Message + "\0"))
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
            fixed (byte* errorPtr = System.Text.Encoding.UTF8.GetBytes(ex.Message + "\0"))
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
}