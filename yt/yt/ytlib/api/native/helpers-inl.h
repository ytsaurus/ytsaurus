#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

template <class TCallback>
auto CallAndRetryIfMetadataCacheIsInconsistent(
    const IConnectionPtr& connection,
    const TDetailedProfilingInfoPtr& profilingInfo,
    const NLogging::TLogger& logger,
    TCallback&& callback) -> decltype(callback())
{
    int retryCount = 0;
    while (true) {
        TError error;

        try {
            return callback();
        } catch (const NYT::TErrorException& ex) {
            error = ex.Error();
        }

        auto delay = InvalidateMountCacheAndGetRetryDelay(
            connection,
            profilingInfo,
            logger,
            error,
            &retryCount);

        if (delay) {
            NConcurrency::TDelayedExecutor::WaitForDuration(delay);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
