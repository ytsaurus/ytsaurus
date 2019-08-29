#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/core/logging/log.h>
#include <yt/core/rpc/public.h>

#include <yt/client/cell_master_client/public.h>

namespace NYT::NCellMasterClient {

///////////////////////////////////////////////////////////////////////////////

class TCellDirectory
    : public TRefCounted
{
public:
    TCellDirectory(
        TCellDirectoryConfigPtr config,
        const NApi::NNative::TConnectionOptions& options,
        const NRpc::IChannelFactoryPtr& channelFactory,
        const NLogging::TLogger& logger);

    void Update(const NCellMasterClient::NProto::TCellDirectory& protoDirectory);

    NObjectClient::TCellId GetPrimaryMasterCellId() const;
    NObjectClient::TCellTag GetPrimaryMasterCellTag() const;
    const NObjectClient::TCellTagList& GetSecondaryMasterCellTags() const;

    NRpc::IChannelPtr GetMasterChannelOrThrow(
        NApi::EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTag);
    NRpc::IChannelPtr GetMasterChannelOrThrow(
        NApi::EMasterChannelKind kind,
        NObjectClient::TCellId cellId);

    NObjectClient::TCellId PickRandomTransactionCoordinatorMasterCell() const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TCellDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient
