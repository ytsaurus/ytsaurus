#include <gtest/gtest.h>

#include <gmock/gmock.h>

#include <library/cpp/yt/backtrace/backtrace.h>

#include <library/cpp/yt/backtrace/cursors/libunwind/libunwind_cursor.h>

namespace NYT::NBacktrace {
namespace {

////////////////////////////////////////////////////////////////////////////////

using ::testing::ContainsRegex;

TString GetStackTrace()
{
    std::array<const void*, 1> buffer;
    NBacktrace::TLibunwindCursor cursor;
    auto backtrace = GetBacktrace(
        &cursor,
        MakeMutableRange(buffer),
        /*framesToSkip*/ 0);
    return SymbolizeBacktrace(backtrace);
}

TEST(TStackTrace, Format)
{
    ASSERT_THAT(
        GetStackTrace(),
        ContainsRegex(
            "^ 1\\. 0x[0-9a-f]+ in NYT::NBacktrace::\\(anonymous namespace\\)::GetStackTrace.* "
            "at .+/library/cpp/yt/backtrace/symbolizers/dwarf/unittests/backtrace_ut.cpp:[0-9]+\\n"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NBacktrace
