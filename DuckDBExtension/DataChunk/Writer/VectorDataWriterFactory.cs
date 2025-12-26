using DuckDB.Extension;

namespace DuckDB.NET.Data.DataChunk.Writer;

internal static class VectorDataWriterFactory
{
    public static unsafe VectorDataWriterBase CreateWriter(IntPtr vector, DuckDBLogicalType logicalType)
    {
        var dataPointer = NativeMethods.Vectors.DuckDBVectorGetData(vector);
        var columnType = NativeMethods.LogicalType.DuckDBGetTypeId(logicalType);

        return columnType switch
        {
            DuckDBType.Uuid => new GuidVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.Date => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
            DuckDBType.Time => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
            DuckDBType.TimeTz => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
            DuckDBType.Interval => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
            DuckDBType.Timestamp => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
                
            DuckDBType.Boolean => new BooleanVectorDataWriter(vector, dataPointer, columnType),

            DuckDBType.Map => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
            DuckDBType.List => new ListVectorDataWriter(vector, dataPointer, columnType, logicalType),
            DuckDBType.Array => new ListVectorDataWriter(vector, dataPointer, columnType, logicalType),
            DuckDBType.Blob => new StringVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.Varchar => new StringVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.Bit => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
            DuckDBType.Enum => new EnumVectorDataWriter(vector, dataPointer, logicalType, columnType),
            DuckDBType.Struct => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
            DuckDBType.Decimal => new DecimalVectorDataWriter(vector, dataPointer, logicalType, columnType),
            DuckDBType.TimestampS => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
            DuckDBType.TimestampMs => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
            DuckDBType.TimestampNs => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
            DuckDBType.TimestampTz => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
            _ => new NumericVectorDataWriter(vector, dataPointer, columnType)
        };
    }
}