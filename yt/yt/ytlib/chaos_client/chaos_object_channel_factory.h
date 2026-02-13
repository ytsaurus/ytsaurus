#pragma once

#include "public.h"

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/client/chaos_client/public.h>

namespace NYT::NChaosClient {

struct IChaosObjectChannelFactory
    : public virtual TRefCounted
{
    virtual NRpc::IChannelPtr CreateChannel(
        TReplicationCardId replicationCardId,
        NHydra::EPeerKind peerKind) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosObjectChannelFactory)

////////////////////////////////////////////////////////////////////////////////

IChaosObjectChannelFactoryPtr CreateChaosObjectChannelFactory(
    NHiveClient::ICellDirectoryPtr cellDirectory,
    IChaosResidencyCachePtr residencyCache,
    IChaosCellDirectorySynchronizerPtr synchronizer,
    TChaosObjectChannelConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
