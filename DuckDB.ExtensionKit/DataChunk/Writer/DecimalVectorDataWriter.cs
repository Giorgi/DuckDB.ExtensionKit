using DuckDB.ExtensionKit.Extensions;
using DuckDB.ExtensionKit.Native;

namespace DuckDB.ExtensionKit.DataChunk.Writer;

internal sealed unsafe class DecimalVectorDataWriter(IntPtr vector, void* vectorData, DuckDBLogicalType logicalType, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    private readonly byte targetColumnScale = NativeMethods.NativeMethods.LogicalType.DuckDBDecimalScale(logicalType);
    private readonly DuckDBType decimalType = NativeMethods.NativeMethods.LogicalType.DuckDBDecimalInternalType(logicalType);

    internal override bool AppendDecimal(decimal value, ulong rowIndex)
    {
        switch (decimalType)
        {
            case DuckDBType.SmallInt:
                AppendValueInternal((short)decimal.Multiply(value, DecimalExtensions.PowersOfTen[targetColumnScale]), rowIndex);
                break;
            case DuckDBType.Integer:
                AppendValueInternal((int)decimal.Multiply(value, DecimalExtensions.PowersOfTen[targetColumnScale]), rowIndex);
                break;
            case DuckDBType.BigInt:
                AppendValueInternal((long)decimal.Multiply(value, DecimalExtensions.PowersOfTen[targetColumnScale]), rowIndex);
                break;
            case DuckDBType.HugeInt:
                var mantissa = value.GetMantissa();

                // Rescale: mantissa is value × 10^valueScale, DuckDB needs value × 10^targetColumnScale.
                if (targetColumnScale > value.Scale)
                    mantissa *= DecimalExtensions.BigIntPowersOfTen[targetColumnScale - value.Scale];
                else if (targetColumnScale < value.Scale)
                    mantissa /= DecimalExtensions.BigIntPowersOfTen[value.Scale - targetColumnScale];

                AppendValueInternal(new DuckDBHugeInt(mantissa), rowIndex);
                break;
        }

        return true;
    }
}
