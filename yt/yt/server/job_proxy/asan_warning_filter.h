#include <util/stream/output.h>
#include <util/generic/string.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

// Output stream that skips the ASAN warning of form
// "==123==WARNING: ASan is ignoring requested __asan_handle_no_return:..."
// if it's written by a single Write() call.
class TAsanWarningFilter
    : public IOutputStream
{
public:
    explicit TAsanWarningFilter(IOutputStream* underlying);

private:
    virtual void DoWrite(const void* buf, size_t len) override;
    virtual void DoFlush() override;
    virtual void DoFinish() override;

private:
    IOutputStream* const Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
