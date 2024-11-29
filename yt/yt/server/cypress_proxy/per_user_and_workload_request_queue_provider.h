#pragma once

#include "public.h"

#include <yt/yt/core/rpc/per_key_request_queue_provider.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

class TPerUserAndWorkloadRequestQueueProvider
    : public NRpc::TPerKeyRequestQueueProvider<std::pair<std::string, EUserWorkloadType>>
{
public:
    explicit TPerUserAndWorkloadRequestQueueProvider(
        TReconfigurationCallback reconfigurationCallback = {});

private:
    using TBase = TPerKeyRequestQueueProvider<TKey>;

    NRpc::TRequestQueuePtr CreateQueueForKey(const TKey& userNameAndWorkloadType) override;
    bool IsReconfigurationPermitted(const TKey& userNameAndWorkloadType) const override;

    static TKeyFromRequestHeaderCallback CreateKeyFromRequestHeaderCallback();
};

DEFINE_REFCOUNTED_TYPE(TPerUserAndWorkloadRequestQueueProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
