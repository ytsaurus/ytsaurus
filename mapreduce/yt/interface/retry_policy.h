#pragma once

#include "errors.h"

#include <util/datetime/base.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/ptr.h>


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// IRequestRetryPolicy class controls retries of single request.
class IRequestRetryPolicy
    : public virtual TThrRefBase
{
public:
    virtual ~IRequestRetryPolicy() = default;

    // Helper function that returns text description of current attempt, e.g.
    //   "attempt 3 / 10"
    // used in logs.
    virtual TString GetAttemptDescription() const = 0;

    // Library code calls this function before any request attempt.
    virtual void NotifyNewAttempt() = 0;

    // Return Nothing() if retries must not be continued.
    virtual TMaybe<TDuration> OnGenericError(const yexception& e) = 0;
    virtual TMaybe<TDuration> OnRetriableError(const TErrorResponse& e) = 0;
    virtual void OnIgnoredError(const TErrorResponse& /*e*/) = 0;
};
using IRequestRetryPolicyPtr = ::TIntrusivePtr<IRequestRetryPolicy>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

