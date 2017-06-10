#include "safe_assert.h"

#include <yt/core/concurrency/fls.h>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TAssertionFailedException::TAssertionFailedException(
    const Stroka& expression,
    const Stroka& stackTrace,
    const TNullable<Stroka>& corePath)
    : Expression_(expression)
    , StackTrace_(stackTrace)
    , CorePath_(corePath)
{ }

////////////////////////////////////////////////////////////////////////////////

struct TSafeAssertionsContext
{
    TCoreDumperPtr CoreDumper;
    TAsyncSemaphorePtr CoreSemaphore;
};

TNullable<TSafeAssertionsContext>& SafeAssertionsContext()
{
    static TFls<TNullable<TSafeAssertionsContext>> context;
    return *context;
}

////////////////////////////////////////////////////////////////////////////////

TSafeAssertionsGuard::TSafeAssertionsGuard(TCoreDumperPtr coreDumper, TAsyncSemaphorePtr coreSemaphore)
{
    Active_ = static_cast<bool>(coreDumper) &&
        static_cast<bool>(coreSemaphore) &&
        !SafeAssertionsModeEnabled();
    if (Active_) {
        SetSafeAssertionsMode(std::move(coreDumper), std::move(coreSemaphore));
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
        ResetSafeAssertionsMode();
        Active_ = false;
    }
}

////////////////////////////////////////////////////////////////////////////////

void SetSafeAssertionsMode(TCoreDumperPtr coreDumper, TAsyncSemaphorePtr coreSemaphore)
{
    // NB: If the condition is not held, YCHECK will actually happen in safe mode,
    // throwing an exception (possibly, failing an innocent operation controller, or
    // something else). This behaviour is intended.
    YCHECK(!SafeAssertionsModeEnabled());
    SafeAssertionsContext().Assign(TSafeAssertionsContext{std::move(coreDumper), std::move(coreSemaphore)});
}

bool SafeAssertionsModeEnabled()
{
    return SafeAssertionsContext().HasValue();
}

TCoreDumperPtr GetSafeAssertionsCoreDumper()
{
    return SafeAssertionsContext()->CoreDumper;
}

TAsyncSemaphorePtr GetSafeAssertionsCoreSemaphore()
{
    return SafeAssertionsContext()->CoreSemaphore;
}

void ResetSafeAssertionsMode()
{
    YCHECK(SafeAssertionsModeEnabled());
    SafeAssertionsContext().Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
