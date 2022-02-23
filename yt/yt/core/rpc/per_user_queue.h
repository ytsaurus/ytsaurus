#pragma once

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TPerUserRequestQueue
{
public:
    TRequestQueueProvider GetProvider();

private:
    TRequestQueuePtr GetOrCreateUserQueue(const TString& name);

    NConcurrency::TSyncMap<TString, TRequestQueuePtr> RequestQueues_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
