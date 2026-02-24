using DuckDB.ExtensionKit.DataChunk.Reader;
using DuckDB.ExtensionKit.DataChunk.Writer;
using DuckDB.ExtensionKit.Native;

namespace DuckDB.ExtensionKit.ScalarFunctions;

public static class ScalarFunctionHighLevelExtensions
{
    extension(DuckDBConnection connection)
    {
        public void RegisterScalarFunction<TResult>(string name, Func<TResult> func, bool isPureFunction = false)
        {
            connection.RegisterScalarFunction<TResult>(name, (writer, rowCount) =>
            {
                for (ulong index = 0; index < rowCount; index++)
                {
                    writer.WriteValue(func(), index);
                }
            }, isPureFunction);
        }

        public void RegisterScalarFunction<T, TResult>(string name, Func<T, TResult> func, bool isPureFunction = true)
        {
            connection.RegisterScalarFunction<T, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(readers[0].GetValue<T>(index))), isPureFunction);
        }

        public void RegisterScalarFunction<T1, T2, TResult>(string name, Func<T1, T2, TResult> func, bool isPureFunction = true)
        {
            connection.RegisterScalarFunction<T1, T2, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(readers[0].GetValue<T1>(index), readers[1].GetValue<T2>(index))), isPureFunction);
        }

        public void RegisterScalarFunction<T1, T2, T3, TResult>(string name, Func<T1, T2, T3, TResult> func, bool isPureFunction = true)
        {
            connection.RegisterScalarFunction<T1, T2, T3, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(readers[0].GetValue<T1>(index), readers[1].GetValue<T2>(index), readers[2].GetValue<T3>(index))), isPureFunction);
        }

        public void RegisterScalarFunction<T1, T2, T3, T4, TResult>(string name, Func<T1, T2, T3, T4, TResult> func, bool isPureFunction = true)
        {
            connection.RegisterScalarFunction<T1, T2, T3, T4, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(readers[0].GetValue<T1>(index), readers[1].GetValue<T2>(index),
                    readers[2].GetValue<T3>(index), readers[3].GetValue<T4>(index))), isPureFunction);
        }
    }

    private static Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> WrapScalarFunction<TResult>(Func<IReadOnlyList<IDuckDBDataReader>, ulong, TResult> perRowFunc)
    {
        return (readers, writer, rowCount) =>
        {
            for (ulong index = 0; index < rowCount; index++)
            {
                var result = perRowFunc(readers, index);
                writer.WriteValue(result, index);
            }
        };
    }
}
