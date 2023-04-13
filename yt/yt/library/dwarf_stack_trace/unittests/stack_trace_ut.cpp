#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/raw_formatter.h>
#include <yt/yt/core/misc/stack_trace.h>

#include <library/cpp/yt/backtrace/helpers.h>

#include <library/cpp/yt/backtrace/cursors/libunwind/libunwind_cursor.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

using ::testing::ContainsRegex;

TString GetStackTrace()
{
    std::array<const void*, 1> buffer;
    NBacktrace::TLibunwindCursor cursor;
    auto frames = NBacktrace::GetBacktraceFromCursor(
        &cursor,
        MakeMutableRange(buffer),
        /*framesToSkip*/ 0);
    return FormatStackTrace(frames.begin(), frames.size());
}

TEST(TStackTrace, Format)
{
    ASSERT_THAT(
        GetStackTrace(),
        ContainsRegex(
            "^ 1\\. 0x[0-9a-f]+ in NYT::\\(anonymous namespace\\)::GetStackTrace.* "
            "at .+/yt/yt/library/dwarf_stack_trace/unittests/stack_trace_ut.cpp:[0-9]+\\n"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
