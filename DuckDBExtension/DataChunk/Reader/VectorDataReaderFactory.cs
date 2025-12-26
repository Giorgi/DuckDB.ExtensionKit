using DuckDB.Extension;

namespace DuckDB.NET.Data.DataChunk.Reader;

internal static class VectorDataReaderFactory
{
    public static unsafe VectorDataReaderBase CreateReader(IntPtr vector, DuckDBLogicalType logicalColumnType, string columnName = "")
    {
        var columnType = NativeMethods.LogicalType.DuckDBGetTypeId(logicalColumnType);
        var dataPointer = NativeMethods.Vectors.DuckDBVectorGetData(vector);
        var validityMaskPointer = NativeMethods.Vectors.DuckDBVectorGetValidity(vector);

        return columnType switch
        {
            DuckDBType.Uuid => new GuidVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Date or DuckDBType.Time or DuckDBType.TimeTz or DuckDBType.Interval or
            DuckDBType.Timestamp or DuckDBType.TimestampMs or DuckDBType.TimestampNs or DuckDBType.TimestampTz
                => throw new NotImplementedException($"Reading {columnType} columns is not implemented"),

            DuckDBType.Boolean => new BooleanVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),

            DuckDBType.Map => new MapVectorDataReader(vector, dataPointer, validityMaskPointer, columnType, logicalColumnType, columnName),
            DuckDBType.List => new ListVectorDataReader(vector, dataPointer, validityMaskPointer, columnType, logicalColumnType, columnName),
            DuckDBType.Array => new ListVectorDataReader(vector, dataPointer, validityMaskPointer, columnType, logicalColumnType, columnName),
            DuckDBType.Blob => new StringVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Varchar => new StringVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Bit => new StringVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Enum => new EnumVectorDataReader(vector, dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Struct => throw new NotImplementedException("Reading struct columns is not implemented"),
            DuckDBType.Decimal => new DecimalVectorDataReader(vector, dataPointer, validityMaskPointer, columnType, columnName),
            _ => new NumericVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName)
        };
    }
}