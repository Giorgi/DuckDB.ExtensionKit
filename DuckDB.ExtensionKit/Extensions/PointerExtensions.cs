using System.Runtime.InteropServices.Marshalling;

namespace DuckDB.ExtensionKit.Extensions;

internal static class PointerExtensions
{
    internal static unsafe string ToManagedString(byte* unmanagedString, bool freeWhenCopied = true, int? length = null)
    {
        var result = Utf8StringMarshaller.ConvertToManaged(unmanagedString);

        if (freeWhenCopied)
        {
            NativeMethods.NativeMethods.Helpers.DuckDBFree(unmanagedString);
        }

        return result ?? "";
    }
}