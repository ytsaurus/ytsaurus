#include <yt/core/test_framework/framework.h>

#include <yt/server/job_proxy/asan_warning_filter.h>

namespace NYT {
namespace NJobProxy {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TAsanWarningFilter, SkipsWarning)
{
    const TStringBuf message =
        "==777==WARNING: ASan is ignoring requested __asan_handle_no_return: "
        "stack top: 0x7ffeecef0000; bottom 0x7fa46f18e000; size: 0x005a7dd62000 (388658241536)\n"
        "False positive error reports may follow\n"
        "For details see https://github.com/google/sanitizers/issues/189\n";

    TStringStream output;
    {
        TAsanWarningFilter stream(&output);
        stream << message;
        stream << "NON-WARNING";
        stream.Finish();
    }
    ASSERT_EQ(output.Str(), "NON-WARNING");
}

TEST(TAsanWarningFilter, RetainsNonWarning)
{
    const TStringBuf message =
        "xxx"
        "==777==WARNING: ASan is ignoring requested __asan_handle_no_return: "
        "stack top: 0x7ffeecef0000; bottom 0x7fa46f18e000; size: 0x005a7dd62000 (388658241536)\n"
        "False positive error reports may follow\n"
        "For details see https://github.com/google/sanitizers/issues/189\n";

    TStringStream output;
    {
        TAsanWarningFilter stream(&output);
        stream << message;
        stream.Finish();
    }
    // Filter skips the warning only if it is written with a single write.
    // Here it is prefixed with another string, so has been retained.
    ASSERT_EQ(output.Str(), message);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NJobProxy
} // namespace NYT
