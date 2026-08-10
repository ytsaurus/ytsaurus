#pragma once

#include "private.h"

#include <yt/yt/core/rpc/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TMaterializedViewCoordinator
    : public TRefCounted
{
public:
    TMaterializedViewCoordinator(
        THost* host,
        TCypressObjectRepositoryPtr repository,
        TMaterializedViewsConfigPtr config,
        NRpc::IChannelFactoryPtr channelFactory);
    ~TMaterializedViewCoordinator();

    void Start();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TMaterializedViewCoordinator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
