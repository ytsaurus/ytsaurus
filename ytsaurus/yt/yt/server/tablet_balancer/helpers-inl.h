#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
void ExecuteRequestsToCellTags(THashMap<NObjectClient::TCellTag, TRequest>* batchRequests)
{
    std::vector<TFuture<void>> futures;
    for (auto& [cellTag, batchRequests] : *batchRequests) {
        batchRequests.Response = batchRequests.Request->Invoke();
        futures.push_back(batchRequests.Response.AsVoid());
    }

    NConcurrency::WaitFor(AllSucceeded(std::move(futures)))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
