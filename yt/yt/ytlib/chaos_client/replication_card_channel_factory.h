#pragma once

#include "public.h"

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/client/chaos_client/public.h>

namespace NYT::NChaosClient {

struct IReplicationCardChannelFactory
    : public virtual TRefCounted
{
    virtual NRpc::IChannelPtr CreateChannel(
        TReplicationCardId replicationCardId,
        NHydra::EPeerKind peerKind) = 0;
};

DEFINE_REFCOUNTED_TYPE(IReplicationCardChannelFactory)

////////////////////////////////////////////////////////////////////////////////

IReplicationCardChannelFactoryPtr CreateReplicationCardChannelFactory(
    NHiveClient::ICellDirectoryPtr cellDirectory,
    IReplicationCardResidencyCachePtr residencyCache,
    IChaosCellDirectorySynchronizerPtr synchronizer,
    TReplicationCardChannelConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
