#include "retryable_client_spec.h"

namespace NYT::NFlow {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TDynamicRetryableClientSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("min_inner_timeout", &TThis::MinInnerTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("timeout", &TThis::Timeout)
        .Default(TDuration::Seconds(300));
    registrar.Parameter("backoff", &TThis::Backoff)
        .Default(TExponentialBackoffOptions{
            .InvocationCount = 30,
            .MinBackoff = TDuration::Seconds(1),
            .MaxBackoff = TDuration::Seconds(60),
            .BackoffMultiplier = 2.0,
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
