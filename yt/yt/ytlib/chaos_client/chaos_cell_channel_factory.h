#pragma once

#include "public.h"

#include <yt/yt/ytlib/hive/public.h>

namespace NYT::NChaosClient {

struct IChaosCellChannelFactory
    : public TRefCounted
{
    virtual NRpc::IChannelPtr CreateChannel(
        NObjectClient::TCellId cellId,
        NHydra::EPeerKind peerKind) = 0;

    virtual NRpc::IChannelPtr CreateChannel(
        NObjectClient::TCellTag cellTag,
        NHydra::EPeerKind peerKind) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosCellChannelFactory)

////////////////////////////////////////////////////////////////////////////////

IChaosCellChannelFactoryPtr CreateChaosCellChannelFactory(
    NHiveClient::ICellDirectoryPtr cellDirectory,
    IChaosCellDirectorySynchronizerPtr synchronizer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
