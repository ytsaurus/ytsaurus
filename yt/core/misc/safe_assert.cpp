#include "safe_assert.h"

#include <yt/core/concurrency/fls.h>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TAssertionFailedException::TAssertionFailedException(
    const TString& expression,
    const TString& stackTrace,
    const TNullable<TString>& corePath)
    : Expression_(expression)
    , StackTrace_(stackTrace)
    , CorePath_(corePath)
{ }

////////////////////////////////////////////////////////////////////////////////

struct TSafeAssertionsFrame
{
    TCoreDumperPtr CoreDumper;
    TAsyncSemaphorePtr CoreSemaphore;
    std::vector<TString> CoreNotes;
};

//! This vector keeps all information about safe frames we have in our stack. The resulting
//! `CoreDumper` and `CoreSemaphore` are taken from the last frame, the resulting `CoreNotes` is
//! a union of `CoreNotes` for all frames.
//! If the vector is empty, safe assertions mode is disabled.
typedef std::vector<TSafeAssertionsFrame> TSafeAssertionsContext;

TSafeAssertionsContext& SafeAssertionsContext()
{
    static TFls<TSafeAssertionsContext> context;
    return *context;
}

////////////////////////////////////////////////////////////////////////////////

TSafeAssertionsGuard::TSafeAssertionsGuard(
    TCoreDumperPtr coreDumper,
    TAsyncSemaphorePtr coreSemaphore,
    std::vector<TString> coreNotes)
{
    Active_ = static_cast<bool>(coreDumper) &&
        static_cast<bool>(coreSemaphore);
    if (Active_) {
        PushSafeAssertionsFrame(std::move(coreDumper), std::move(coreSemaphore), std::move(coreNotes));
    }
}

TSafeAssertionsGuard::~TSafeAssertionsGuard()
{
    Release();
}

TSafeAssertionsGuard::TSafeAssertionsGuard(TSafeAssertionsGuard&& other)
    : Active_(other.Active_)
{
    other.Active_ = false;
}

TSafeAssertionsGuard& TSafeAssertionsGuard::operator=(TSafeAssertionsGuard&& other)
{
    if (this != &other) {
        Release();
        Active_ = other.Active_;
        other.Active_ = false;
    }
    return *this;
}

void TSafeAssertionsGuard::Release()
{
    if (Active_) {
        PopSafeAssertionsFrame();
        Active_ = false;
    }
}

////////////////////////////////////////////////////////////////////////////////

void PushSafeAssertionsFrame(TCoreDumperPtr coreDumper, TAsyncSemaphorePtr coreSemaphore, std::vector<TString> coreNotes)
{
    SafeAssertionsContext().emplace_back(
        TSafeAssertionsFrame{std::move(coreDumper), std::move(coreSemaphore), std::move(coreNotes)});
}

bool SafeAssertionsModeEnabled()
{
    return !SafeAssertionsContext().empty();
}

TCoreDumperPtr GetSafeAssertionsCoreDumper()
{
    return SafeAssertionsContext().back().CoreDumper;
}

TAsyncSemaphorePtr GetSafeAssertionsCoreSemaphore()
{
    return SafeAssertionsContext().back().CoreSemaphore;
}

std::vector<TString> GetSafeAssertionsCoreNotes()
{
    std::vector<TString> coreNotes;
    for (const auto& frame : SafeAssertionsContext()) {
        coreNotes.insert(coreNotes.end(), frame.CoreNotes.begin(), frame.CoreNotes.end());
    }
    return coreNotes;
}

void PopSafeAssertionsFrame()
{
    SafeAssertionsContext().pop_back();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
