#include "asan_warning_filter.h"

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

static const TString WarningPrefix = "==";
static const TString WarningSuffix =
    "False positive error reports may follow\n"
    "For details see https://github.com/google/sanitizers/issues/189\n";

////////////////////////////////////////////////////////////////////////////////

TAsanWarningFilter::TAsanWarningFilter(IOutputStream* underlying)
    : Underlying_(underlying)
{ }

void TAsanWarningFilter::DoWrite(const void* buf, size_t len)
{
    TStringBuf buffer(static_cast<const char*>(buf), len);
    if (buffer.StartsWith(WarningPrefix) && buffer.EndsWith(WarningSuffix)) {
        return;
    }
    Underlying_->Write(buf, len);
}

void TAsanWarningFilter::DoFlush()
{
    Underlying_->Flush();
}

void TAsanWarningFilter::DoFinish()
{
    Underlying_->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
