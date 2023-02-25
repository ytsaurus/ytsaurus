#pragma once

#include <yt/cpp/mapreduce/interface/client.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template<typename TSrc, typename TBatchAdder>
auto BatchTransform(
    const IClientBasePtr& client,
    const TSrc& src,
    TBatchAdder batchAdder,
    const TExecuteBatchOptions& executeBatchOptions = {})
{
    auto batch = client->CreateBatchRequest();
    using TFuture = decltype(batchAdder(batch, *std::begin(src)));
    TVector<TFuture> futures;
    for (const auto& el : src) {
        futures.push_back(batchAdder(batch, el));
    }
    batch->ExecuteBatch(executeBatchOptions);
    using TDst = decltype(futures[0].ExtractValueSync());
    TVector<TDst> result;
    result.reserve(std::size(src));
    for (auto& future : futures) {
        result.push_back(future.ExtractValueSync());
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
