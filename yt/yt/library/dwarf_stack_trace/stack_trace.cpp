#include <library/cpp/dwarf_backtrace/backtrace.h>

#include <yt/yt/core/misc/raw_formatter.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Alternative implementation for `FormatStackTrace` via library `library/cpp/dwarf_backtrace`.
void FormatStackTrace(const void* const* frames, int frameCount, std::function<void(TStringBuf)> callback)
{
    TRawFormatter<1024> formatter;
    auto error = NDwarf::ResolveBacktrace({frames, static_cast<size_t>(frameCount)}, [&] (const NDwarf::TLineInfo& info) {
        formatter.Reset();
        formatter.AppendNumber(info.Index + 1, 10, 2);
        formatter.AppendString(". ");
        formatter.AppendString("0x");
        const int width = (sizeof(void*) == 8 ? 12 : 8);
        // 12 for x86_64 because higher bits are always zeroed.
        formatter.AppendNumber(info.Address, 16, width, '0');
        formatter.AppendString(" in ");
        formatter.AppendString(info.FunctionName);
        formatter.AppendString(" at ");
        formatter.AppendString(info.FileName);
        formatter.AppendChar(':');
        formatter.AppendNumber(info.Line);
        formatter.AppendString("\n");
        callback(formatter.GetBuffer());
        // Call the callback exactly `frameCount` times,
        // even if there are inline functions and one frame resolved to several lines.
        if (info.Index + 1 == frameCount) {
            return NDwarf::EResolving::Break;
        }
        return NDwarf::EResolving::Continue;
    });
    if (error) {
        formatter.Reset();
        formatter.AppendString("***Cannot get backtrace (code=");
        formatter.AppendNumber(error->Code);
        formatter.AppendString(", message=");
        formatter.AppendString(error->Message);
        formatter.AppendString(")***");
        callback(formatter.GetBuffer());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
