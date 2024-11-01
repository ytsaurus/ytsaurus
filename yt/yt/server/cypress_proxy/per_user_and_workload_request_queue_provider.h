#pragma once

#include "public.h"

#include <yt/yt/core/rpc/per_key_request_queue_provider.h>

namespace NYT::NCypressProxy {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TPerUserAndWorkloadRequestQueueProvider
    : public TPerKeyRequestQueueProvider<std::pair<std::string, EUserWorkloadType>>
{
public:
    using TBase = TPerKeyRequestQueueProvider<TKey>;

    TPerUserAndWorkloadRequestQueueProvider(
        TReconfigurationCallback reconfigurationCallback = {});

private:
    TRequestQueuePtr CreateQueueForKey(const TKey& userNameAndWorkloadType) override;
    bool IsReconfigurationPermitted(const TKey& userNameAndWorkloadType) const override;

    static TKeyFromRequestHeaderCallback CreateKeyFromRequestHeaderCallback();
};

DECLARE_REFCOUNTED_CLASS(TPerUserAndWorkloadRequestQueueProvider);
DEFINE_REFCOUNTED_TYPE(TPerUserAndWorkloadRequestQueueProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
