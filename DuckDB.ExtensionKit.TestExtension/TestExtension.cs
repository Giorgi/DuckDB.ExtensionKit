using DuckDB.ExtensionKit;
using DuckDB.ExtensionKit.Native;
using DuckDB.ExtensionKit.ScalarFunctions;
using DuckDB.ExtensionKit.TableFunctions;
using DuckDB.NET.Native;
using System.Numerics;
using System.Text;

namespace DuckDB.ExtensionKit.TestExtension;

// The functions that DuckDB.ExtensionKit.Tests calls. Each kind of value and of registration has one, so that the
// tests run every path of the readers, the writers and the registration code in an extension built with Native AOT,
// where reflection and code built at run time can fail although they work with the JIT.
[DuckDBExtension]
public static partial class TestExtension
{
    private static void RegisterFunctions(DuckDBConnection connection)
    {
        RegisterEchoFunctions(connection);
        RegisterNullHandlingFunctions(connection);
        RegisterTableFunctions(connection);
    }

    // echo_<type>(x) returns x: a value read as the .NET type and written back as it.
    private static void RegisterEchoFunctions(DuckDBConnection connection)
    {
        connection.RegisterScalarFunction<bool, bool>("echo_bool", (bool v) => v);
        connection.RegisterScalarFunction<sbyte, sbyte>("echo_sbyte", (sbyte v) => v);
        connection.RegisterScalarFunction<short, short>("echo_short", (short v) => v);
        connection.RegisterScalarFunction<int, int>("echo_int", (int v) => v);
        connection.RegisterScalarFunction<long, long>("echo_long", (long v) => v);
        connection.RegisterScalarFunction<byte, byte>("echo_byte", (byte v) => v);
        connection.RegisterScalarFunction<ushort, ushort>("echo_ushort", (ushort v) => v);
        connection.RegisterScalarFunction<uint, uint>("echo_uint", (uint v) => v);
        connection.RegisterScalarFunction<ulong, ulong>("echo_ulong", (ulong v) => v);
        connection.RegisterScalarFunction<float, float>("echo_float", (float v) => v);
        connection.RegisterScalarFunction<double, double>("echo_double", (double v) => v);
        connection.RegisterScalarFunction<decimal, decimal>("echo_decimal", (decimal v) => v);
        connection.RegisterScalarFunction<Int128, Int128>("echo_int128", (Int128 v) => v);
        connection.RegisterScalarFunction<UInt128, UInt128>("echo_uint128", (UInt128 v) => v);
        connection.RegisterScalarFunction<BigInteger, BigInteger>("echo_biginteger", (BigInteger v) => v);
        connection.RegisterScalarFunction<string, string>("echo_string", (string v) => v);
        connection.RegisterScalarFunction<Guid, Guid>("echo_guid", (Guid v) => v);
        connection.RegisterScalarFunction<DateOnly, DateOnly>("echo_dateonly", (DateOnly v) => v);
        connection.RegisterScalarFunction<TimeOnly, TimeOnly>("echo_timeonly", (TimeOnly v) => v);
        connection.RegisterScalarFunction<DuckDBDateOnly, DuckDBDateOnly>("echo_duckdbdateonly", (DuckDBDateOnly v) => v);
        connection.RegisterScalarFunction<DuckDBTimeOnly, DuckDBTimeOnly>("echo_duckdbtimeonly", (DuckDBTimeOnly v) => v);
        connection.RegisterScalarFunction<DateTime, DateTime>("echo_datetime", (DateTime v) => v);
        connection.RegisterScalarFunction<DateTimeOffset, DateTimeOffset>("echo_datetimeoffset", (DateTimeOffset v) => v);
        connection.RegisterScalarFunction<TimeSpan, TimeSpan>("echo_timespan", (TimeSpan v) => v);
        connection.RegisterScalarFunction<byte[], byte[]>("echo_bytes", (byte[] v) => v);

        connection.RegisterScalarFunction<List<int>, List<int>>("echo_list_int", (List<int> v) => v);
        connection.RegisterScalarFunction<List<string?>, List<string?>>("echo_list_string", (List<string?> v) => v);
        connection.RegisterScalarFunction<List<int?>, List<int?>>("echo_list_nullable_int", (List<int?> v) => v);
        connection.RegisterScalarFunction<List<DateTime?>, List<DateTime?>>("echo_list_nullable_datetime", (List<DateTime?> v) => v);
        connection.RegisterScalarFunction<List<decimal>, List<decimal>>("echo_list_decimal", (List<decimal> v) => v);
        connection.RegisterScalarFunction<List<Guid>, List<Guid>>("echo_list_guid", (List<Guid> v) => v);

        connection.RegisterScalarFunction<int?, int?>("echo_nullable_int", (int? v) => v);
        connection.RegisterScalarFunction<DateTime?, DateTime?>("echo_nullable_datetime", (DateTime? v) => v);
        connection.RegisterScalarFunction<Int128?, Int128?>("echo_nullable_int128", (Int128? v) => v);
        connection.RegisterScalarFunction<Guid?, Guid?>("echo_nullable_guid", (Guid? v) => v);

        // A BLOB result from a string: "none" gives NULL, "" an empty BLOB.
        connection.RegisterScalarFunction<string, byte[]?>("blob_of", (string s) => s == "none" ? null : Encoding.UTF8.GetBytes(s));

        // object: the value as the .NET type of its column.
        connection.RegisterScalarFunction<object, string>("clr_type", (object v) => v.GetType().Name);
    }

    private static void RegisterNullHandlingFunctions(DuckDBConnection connection)
    {
        // No nullable parameter: a row with a NULL argument gets NULL, and the function is not called for it.
        connection.RegisterScalarFunction<int, int, string>("describe_sum", (int a, int b) => $"sum {a + b}");

        // A nullable parameter: the function is called for NULL too.
        connection.RegisterScalarFunction<int?, string>("describe_nullable", (int? v) => v.HasValue ? $"value {v}" : "no value");

        // params: one argument per row of a list.
        connection.RegisterScalarFunction<int, int>("sum_all", (int[] values) => values.Sum());
    }

    public record Row(int Id, bool Even, long Big, decimal Price, Guid Key, DateTime At, DateTimeOffset AtUtc, TimeSpan Span,
                      DateOnly Day, TimeOnly Time, Int128 Huge, string Name, byte[] Bytes, int? Maybe);

    private static Row MakeRow(int i) => new(i, i % 2 == 0, i * 1_000_000_000L, 1.5m + i, new Guid(i, 0, 0, new byte[8]),
        new DateTime(2024, 2, 29, 13, 45, 30).AddDays(i), new DateTimeOffset(2024, 2, 29, 13, 45, 30, TimeSpan.Zero).AddHours(i),
        TimeSpan.FromMinutes(i), new DateOnly(2024, 2, 29).AddDays(i), new TimeOnly(13, 45, 30).Add(TimeSpan.FromSeconds(i)),
        (Int128)i * long.MaxValue, "row " + i, [1, 2, (byte)i], i % 3 == 0 ? null : i);

    private static void RegisterTableFunctions(DuckDBConnection connection)
    {
        // A column of every supported type, over more rows than one vector holds (2048).
        connection.RegisterTableFunction("rows_of", (int count) => Enumerable.Range(0, count).Select(MakeRow),
            (Row r) => new
            {
                id = r.Id, even = r.Even, big = r.Big, price = r.Price, key = r.Key, ts = r.At, ts_utc = r.AtUtc, span = r.Span,
                day = r.Day, time = r.Time, huge = r.Huge, name = r.Name, bytes = r.Bytes, maybe = r.Maybe
            });

        // Decimal arithmetic and a comparison in the projection, which Native AOT trimming broke.
        connection.RegisterTableFunction("decimal_projection", (int count) => Enumerable.Range(0, count).Select(MakeRow),
            (Row r) => new { id = r.Id, total = r.Price * 2m + 0.25m, discounted = r.Price - 1m, expensive = r.Price > 100m });

        // Date and time arguments, which the old value reader could not read.
        connection.RegisterTableFunction("days_from", (DateTime start, int count) => Enumerable.Range(0, count).Select(i => start.AddDays(i)),
            (DateTime d) => new { day = d });

        // No DateTimeOffset (TIMESTAMPTZ) parameter: DuckDBValue, from DuckDB.NET, reads a TIMESTAMPTZ argument as
        // DateTime and refuses to give it as DateTimeOffset.
        connection.RegisterTableFunction("temporal_args", (DateOnly day, TimeOnly time, TimeSpan span, DateTime at) => new[] { (day, time, span, at) },
            ((DateOnly Day, TimeOnly Time, TimeSpan Span, DateTime At) t) => new { day = t.Day, time = t.Time, span = t.Span, ts = t.At });

        // Named parameters; an omitted one is null.
        connection.RegisterTableFunction("named_args", (int count, [Named("prefix")] string? prefix, [Named] int? step) =>
                Enumerable.Range(0, count).Select(i => (prefix ?? "item") + " " + i * (step ?? 1)),
            (string s) => new { value = s });

        // The data callback is told which columns the query reads.
        connection.RegisterTableFunction("projected_columns", (IReadOnlyList<ProjectedColumn> columns) =>
                new[] { string.Join(",", columns.Select(c => c.Name)) },
            (string s) => new { a = s, b = s, c = s });

        // The low-level registration with the parameter types given as DuckDB types.
        connection.RegisterTableFunction("repeat_text", parameters =>
            {
                var text = parameters[0].GetValue<string>();
                var count = parameters[1].GetValue<int>();
                return new TableFunction([new ColumnInfo("text", typeof(string)), new ColumnInfo("n", typeof(int))],
                    Enumerable.Range(1, count).Select(n => (text, n)).ToList());
            },
            (item, writers, rowIndex) =>
            {
                var (text, n) = ((string, int))item!;
                writers[0].WriteValue(text, rowIndex);
                writers[1].WriteValue(n, rowIndex);
            },
            DuckDBType.Varchar, DuckDBType.Integer);
    }
}
