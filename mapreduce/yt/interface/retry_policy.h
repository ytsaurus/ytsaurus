#pragma once

#include "fwd.h"
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
    // Helper function that returns text description of current attempt, e.g.
    //   "attempt 3 / 10"
    // used in logs.
    virtual TString GetAttemptDescription() const = 0;

    // Library code calls this function before any request attempt.
    virtual void NotifyNewAttempt() = 0;

    // OnRetriableError is called whenever client gets YT error that can be retried (e.g. operation limit exceeded).
    // OnGenericError is called whenever request failed due to generic error like network error.
    //
    // Both methods must return nothing if policy doesn't want to retry this error.
    // Otherwise method should return backoff time.
    virtual TMaybe<TDuration> OnRetriableError(const TErrorResponse& e) = 0;
    virtual TMaybe<TDuration> OnGenericError(const yexception& e) = 0;

    // OnIgnoredError is called whenever client gets an error but is going to ignore it.
    virtual void OnIgnoredError(const TErrorResponse& /*e*/) = 0;
};
using IRequestRetryPolicyPtr = ::TIntrusivePtr<IRequestRetryPolicy>;

////////////////////////////////////////////////////////////////////////////////

// IClientRetryPolicy controls creation of policies for individual requests.
class IClientRetryPolicy
    : public virtual TThrRefBase
{
public:
    virtual IRequestRetryPolicyPtr CreatePolicyForGenericRequest() = 0;
    virtual IRequestRetryPolicyPtr CreatePolicyForStartOperationRequest() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TRetryConfig
{
    // RetriesTimeLimit controls how long retries can go on.
    // If this limit is reached while retry count is not yet exceeded TRequestRetriesTimeout exception is thrown.
    TDuration RetriesTimeLimit = TDuration::Max();
};

class IRetryConfigProvider
    : public virtual TThrRefBase
{
public:
    //
    // CreateRetryConfig is called before ANY request.
    // Returned config controls retries of this request.
    //
    // Must be theread safe since it can be used from different threads
    // to perform internal library requests (e.g. pings).
    //
    // NOTE: some methods (e.g. IClient::Map) involve multiple requests to YT and therefore
    // this method will be called several times during execution of single method.
    virtual TRetryConfig CreateRetryConfig() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

