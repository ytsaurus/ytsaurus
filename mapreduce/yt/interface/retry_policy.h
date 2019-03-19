#pragma once

#include "errors.h"

#include <util/datetime/base.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/ptr.h>


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// IRequestRetryPolicy class controls retries of single request.
class IRetryPolicy
    : public virtual TThrRefBase
{
public:
    virtual ~IRetryPolicy() = default;

    // Helper function that returns text description of current attempt, e.g.
    //   "attempt 3 / 10"
    // used in logs.
    virtual TString GetAttemptDescription() const = 0;

    // Library code calls this function before any request attempt.
    virtual void NotifyNewAttempt() = 0;

    // Return Nothing() if retries must not be continued.
    virtual TMaybe<TDuration> GetRetryInterval(const yexception& e) const = 0;
    virtual TMaybe<TDuration> GetRetryInterval(const TErrorResponse& e) const = 0;
};
using IRetryPolicyPtr = ::TIntrusivePtr<IRetryPolicy>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

