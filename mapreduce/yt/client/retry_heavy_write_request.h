#pragma once

#include <mapreduce/yt/common/fwd.h>

#include <mapreduce/yt/http/requests.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

void RetryHeavyWriteRequest(
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const ITransactionPingerPtr& transactionPinger,
    const TClientContext& context,
    const TTransactionId& parentId,
    THttpHeader& header,
    std::function<THolder<IInputStream>()> streamMaker);

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
