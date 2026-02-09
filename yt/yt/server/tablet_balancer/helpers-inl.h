#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include "multicell_throttler.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
void ExecuteRequestsToCellTags(
    THashMap<NObjectClient::TCellTag, TRequest>* batchRequests,
    const IMulticellThrottlerPtr& throttler)
{
    std::vector<TFuture<void>> futures;
    for (auto& [cellTag, batchRequest] : *batchRequests) {
        batchRequest.Response = throttler->GetThrottler(cellTag)->Throttle(batchRequest.GetSize())
            .Apply(BIND(&std::remove_reference_t<decltype(*batchRequest.Request)>::Invoke, batchRequest.Request));
        futures.push_back(batchRequest.Response.AsVoid());
    }

    NConcurrency::WaitFor(AllSucceeded(std::move(futures)))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
