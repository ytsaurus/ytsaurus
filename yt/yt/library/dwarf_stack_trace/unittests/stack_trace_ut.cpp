#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/libunwind/libunwind.h>
#include <yt/yt/core/misc/raw_formatter.h>
#include <yt/yt/core/misc/stack_trace.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

using ::testing::ContainsRegex;

TString GetStackTrace(int limit)
{
    std::array<void*, 2> frames;
    NLibunwind::GetStackTrace(frames.data(), frames.size(), 0);
    return FormatStackTrace(frames.data(), limit);
}

TEST(TStackTrace, Format)
{
    const auto stack = GetStackTrace(2);
    ASSERT_THAT(stack,
        ContainsRegex("^ 1\\. 0x[0-9a-f]+ in NYT::\\(anonymous namespace\\)::GetStackTrace.* "
                      "at .+/yt/yt/library/dwarf_stack_trace/unittests/stack_trace_ut.cpp:17\\n 2. "));
}

TEST(TStackTrace, LinesCountLimit)
{
    const auto stack = GetStackTrace(1);
    ASSERT_THAT(stack,
        ContainsRegex("^ 1\\. 0x[0-9a-f]+ in NYT::\\(anonymous namespace\\)::GetStackTrace.* "
                      "at .+/yt/yt/library/dwarf_stack_trace/unittests/stack_trace_ut.cpp:17\\n$"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
