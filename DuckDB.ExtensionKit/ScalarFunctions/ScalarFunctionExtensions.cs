using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.ExtensionKit.Extensions;
using DuckDB.ExtensionKit.Native;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace DuckDB.ExtensionKit.ScalarFunctions;

public static class ScalarFunctionExtensions
{
    public static void RegisterScalarFunction<TResult>(this DuckDBConnection connection, string name, Action<IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        connection.RegisterScalarMethod(name, (_, w, index) => action(w, index), TypeExtensions.GetLogicalType<TResult>(), varargs: false, options);
    }

    public static void RegisterScalarFunction<T, TResult>(this DuckDBConnection connection, string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null, bool @params = false)
    {
        connection.RegisterScalarMethod(name, action, TypeExtensions.GetLogicalType<TResult>(), @params, options, TypeExtensions.GetLogicalType<T>());
    }

    public static void RegisterScalarFunction<T1, T2, TResult>(this DuckDBConnection connection, string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        connection.RegisterScalarMethod(name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, options,
            TypeExtensions.GetLogicalType<T1>(),
            TypeExtensions.GetLogicalType<T2>());
    }

    public static void RegisterScalarFunction<T1, T2, T3, TResult>(this DuckDBConnection connection, string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        connection.RegisterScalarMethod(name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, options,
            TypeExtensions.GetLogicalType<T1>(),
            TypeExtensions.GetLogicalType<T2>(),
            TypeExtensions.GetLogicalType<T3>());
    }

    public static void RegisterScalarFunction<T1, T2, T3, T4, TResult>(this DuckDBConnection connection, string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        connection.RegisterScalarMethod(name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, options,
            TypeExtensions.GetLogicalType<T1>(),
            TypeExtensions.GetLogicalType<T2>(),
            TypeExtensions.GetLogicalType<T3>(),
            TypeExtensions.GetLogicalType<T4>());
    }

    private static unsafe void RegisterScalarMethod(this DuckDBConnection connection, string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, DuckDBLogicalType returnType,
                                                    bool varargs, ScalarFunctionOptions? options, params DuckDBLogicalType[] parameterTypes)
    {
        var function = NativeMethods.ScalarFunction.DuckDBCreateScalarFunction();

        fixed (byte* namePtr = Encoding.UTF8.GetBytes(name + "\0"))
        {
            NativeMethods.ScalarFunction.DuckDBScalarFunctionSetName(function, namePtr);
        }

        if (varargs)
        {
            if (parameterTypes.Length != 1)
            {
                throw new InvalidOperationException("Cannot use params with multiple parameters");
            }

            NativeMethods.ScalarFunction.DuckDBScalarFunctionSetVarargs(function, parameterTypes[0]);
            parameterTypes[0].Dispose();
        }
        else
        {
            foreach (var type in parameterTypes)
            {
                NativeMethods.ScalarFunction.DuckDBScalarFunctionAddParameter(function, type);
                type.Dispose();
            }
        }

        // Functions with parameters default to pure; parameterless functions (e.g. random()) default to volatile
        var defaultPure = parameterTypes.Length > 0;
        if (!(options?.IsPureFunction ?? defaultPure))
        {
            NativeMethods.ScalarFunction.DuckDBScalarFunctionSetVolatile(function);
        }

        if (options?.HandlesNulls == true)
        {
            NativeMethods.ScalarFunction.DuckDBScalarFunctionSetSpecialHandling(function);
        }

        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetReturnType(function, returnType);

        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetFunction(function, &ScalarFunctionCallback);

        var info = new ScalarFunctionInfo(returnType, action);

        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetExtraInfo(function, info.ToHandle(), &DestroyExtraInfo);

        var state = NativeMethods.ScalarFunction.DuckDBRegisterScalarFunction(connection.Connection, function);

        NativeMethods.ScalarFunction.DuckDBDestroyScalarFunction(&function);

        if (state != DuckDBState.Success)
        {
            throw new InvalidOperationException($"Error registering user defined scalar function: {name}");
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void ScalarFunctionCallback(IntPtr info, IntPtr chunk, IntPtr outputVector)
    {
        VectorDataReaderBase[] readers = [];
        VectorDataWriterBase? writer = null;

        try
        {
            var dataChunk = new DuckDBDataChunk(chunk);

            var chunkSize = NativeMethods.DataChunks.DuckDBDataChunkGetSize(dataChunk);
            var handle =
                GCHandle.FromIntPtr(new IntPtr(NativeMethods.ScalarFunction.DuckDBScalarFunctionGetExtraInfo(info)));

            if (handle.Target is not ScalarFunctionInfo functionInfo)
            {
                throw new InvalidOperationException("User defined scalar function execution failed. Function extra info is null");
            }

            readers = new VectorDataReaderBase[NativeMethods.DataChunks.DuckDBDataChunkGetColumnCount(dataChunk)];

            for (var index = 0; index < readers.Length; index++)
            {
                var vector = NativeMethods.DataChunks.DuckDBDataChunkGetVector(dataChunk, index);
                using var logicalType = NativeMethods.Vectors.DuckDBVectorGetColumnType(vector);
                readers[index] = VectorDataReaderFactory.CreateReader(vector, logicalType);
            }

            writer = VectorDataWriterFactory.CreateWriter(outputVector, functionInfo.ReturnType);

            functionInfo.Action(readers, writer, chunkSize);
        }
        catch (Exception ex)
        {
            fixed (byte* errorPtr = Encoding.UTF8.GetBytes(ex.Message + "\0"))
            {
                NativeMethods.ScalarFunction.DuckDBScalarFunctionSetError(info, errorPtr);
            }
        }
        finally
        {
            foreach (var reader in readers)
            {
                reader?.Dispose();
            }

            writer?.Dispose();
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void DestroyExtraInfo(void* pointer) => new IntPtr(pointer).FreeHandle();
}
