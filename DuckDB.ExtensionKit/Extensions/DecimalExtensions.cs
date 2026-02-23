using System.Numerics;
using System.Runtime.InteropServices;

namespace DuckDB.ExtensionKit.Extensions;

internal static class DecimalExtensions
{
    /// <summary>
    /// Pre-computed powers of 10 as BigInteger values for scales 0-38 (the full range of DuckDB DECIMAL).
    /// </summary>
    internal static readonly BigInteger[] BigIntPowersOfTen = Enumerable.Range(0, 39)
        .Select(i => BigInteger.Pow(10, i))
        .ToArray();

    /// <summary>
    /// Maximum scale supported by .NET <see cref="decimal"/> (10^28 is the largest exact power of ten it can represent).
    /// </summary>
    internal const int MaxDecimalScale = 28;

    /// <summary>
    /// Pre-computed powers of 10 as decimal values for scales 0-28 (the full range of <see cref="decimal"/>).
    /// Avoids <c>(decimal)Math.Pow(10, scale)</c> which loses precision for large scales
    /// due to the double intermediate representation.
    /// </summary>
    internal static readonly decimal[] PowersOfTen =
    [
        1m,
        10m,
        100m,
        1_000m,
        10_000m,
        100_000m,
        1_000_000m,
        10_000_000m,
        100_000_000m,
        1_000_000_000m,
        10_000_000_000m,
        100_000_000_000m,
        1_000_000_000_000m,
        10_000_000_000_000m,
        100_000_000_000_000m,
        1_000_000_000_000_000m,
        10_000_000_000_000_000m,
        100_000_000_000_000_000m,
        1_000_000_000_000_000_000m,
        10_000_000_000_000_000_000m,
        100_000_000_000_000_000_000m,
        1_000_000_000_000_000_000_000m,
        10_000_000_000_000_000_000_000m,
        100_000_000_000_000_000_000_000m,
        1_000_000_000_000_000_000_000_000m,
        10_000_000_000_000_000_000_000_000m,
        100_000_000_000_000_000_000_000_000m,
        1_000_000_000_000_000_000_000_000_000m,
        10_000_000_000_000_000_000_000_000_000m,
    ];

    /// <summary>
    /// Extracts the signed mantissa from the decimal's binary representation.
    /// The mantissa equals value × 10^Scale, avoiding arithmetic reconstruction.
    /// </summary>
    internal static BigInteger GetMantissa(this decimal value)
    {
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value, bits);

        var isNegative = bits[3] < 0;

        Span<byte> mantissaBytes = stackalloc byte[13]; // 12 bytes mantissa + 1 zero for unsigned
        MemoryMarshal.AsBytes(bits[..3]).CopyTo(mantissaBytes);
        mantissaBytes[12] = 0;

        var mantissa = new BigInteger(mantissaBytes, isUnsigned: true, isBigEndian: false);
        if (isNegative) mantissa = -mantissa;

        return mantissa;
    }
}
