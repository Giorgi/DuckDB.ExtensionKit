using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;

namespace DuckDB.Extension;

static class ScalarFunctionExtensions
{
    public static void RegisterScalarFunction<TResult>(this DuckDBNativeConnection connection, string name, Action<IDuckDBDataWriter, ulong> action, bool isPureFunction = false)
    {
        RegisterScalarMethod(connection, name, (_, w, index) => action(w, index), TypeExtensions.GetLogicalType<TResult>(), varargs: false, !isPureFunction);
    }

    public static void RegisterScalarFunction<T, TResult>(this DuckDBNativeConnection connection, string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, bool isPureFunction = true, bool @params = false)
    {
        RegisterScalarMethod(connection, name, action, TypeExtensions.GetLogicalType<TResult>(), @params, !isPureFunction, TypeExtensions.GetLogicalType<T>());
    }

    public static void RegisterScalarFunction<T1, T2, TResult>(this DuckDBNativeConnection connection, string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, bool isPureFunction = true)
    {
        RegisterScalarMethod(connection, name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, !isPureFunction,
                              TypeExtensions.GetLogicalType<T1>(),
                              TypeExtensions.GetLogicalType<T2>());
    }

    public static void RegisterScalarFunction<T1, T2, T3, TResult>(this DuckDBNativeConnection connection, string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, bool isPureFunction = true)
    {
        RegisterScalarMethod(connection, name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, !isPureFunction,
                              TypeExtensions.GetLogicalType<T1>(),
                              TypeExtensions.GetLogicalType<T2>(),
                              TypeExtensions.GetLogicalType<T3>());
    }

    public static void RegisterScalarFunction<T1, T2, T3, T4, TResult>(this DuckDBNativeConnection connection, string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, bool isPureFunction = true)
    {
        RegisterScalarMethod(connection, name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, !isPureFunction,
                              TypeExtensions.GetLogicalType<T1>(),
                              TypeExtensions.GetLogicalType<T2>(),
                              TypeExtensions.GetLogicalType<T3>(),
                              TypeExtensions.GetLogicalType<T4>());
    }

    private static unsafe void RegisterScalarMethod(this DuckDBNativeConnection connection, string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, DuckDBLogicalType returnType,
                                             bool varargs, bool @volatile, params DuckDBLogicalType[] parameterTypes)
    {
        var function = NativeMethods.ScalarFunction.DuckDBCreateScalarFunction();
        
        fixed (byte* namePtr = System.Text.Encoding.UTF8.GetBytes(name + "\0"))
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
        }
        else
        {
            foreach (var type in parameterTypes)
            {
                NativeMethods.ScalarFunction.DuckDBScalarFunctionAddParameter(function, type);
                type.Dispose();
            }
        }

        if (@volatile)
        {
            NativeMethods.ScalarFunction.DuckDBScalarFunctionSetVolatile(function);
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
        var dataChunk = new DuckDBDataChunk(chunk);

        var chunkSize = NativeMethods.DataChunks.DuckDBDataChunkGetSize(dataChunk);
        var handle = GCHandle.FromIntPtr(new IntPtr(NativeMethods.ScalarFunction.DuckDBScalarFunctionGetExtraInfo(info)));

        if (handle.Target is not ScalarFunctionInfo functionInfo)
        {
            throw new InvalidOperationException("User defined scalar function execution failed. Function extra info is null");
        }

        var readers = new VectorDataReaderBase[NativeMethods.DataChunks.DuckDBDataChunkGetColumnCount(dataChunk)];

        for (var index = 0; index < readers.Length; index++)
        {
            var vector = NativeMethods.DataChunks.DuckDBDataChunkGetVector(dataChunk, index);
            readers[index] = VectorDataReaderFactory.CreateReader(vector, NativeMethods.Vectors.DuckDBVectorGetColumnType(vector));
        }

        var writer = VectorDataWriterFactory.CreateWriter(outputVector, functionInfo.ReturnType);

        functionInfo.Action(readers, writer, chunkSize);
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void DestroyExtraInfo(void* pointer) => new IntPtr(pointer).FreeHandle();

}
