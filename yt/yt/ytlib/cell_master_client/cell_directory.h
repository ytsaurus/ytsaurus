#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/api/public.h>

#include <yt/yt/client/cell_master_client/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NCellMasterClient {

///////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateMasterCacheChannel(
    const NApi::NNative::TMasterConnectionConfigPtr& config,
    const NApi::NNative::TMasterConnectionConfigPtr& masterCacheConfig,
    const NRpc::IChannelFactoryPtr channelFactory,
    const NApi::NNative::TConnectionOptions& options,
    const std::vector<TString>& discoveredAddresses);

///////////////////////////////////////////////////////////////////////////////

using TSecondaryMasterConnectionConfigs = THashMap<NObjectClient::TCellTag, NApi::NNative::TMasterConnectionConfigPtr>;

///////////////////////////////////////////////////////////////////////////////

struct ICellDirectory
    : public TRefCounted
{
    using TCellReconfigurationSignature = void(
        const THashSet<NObjectClient::TCellTag>& /*addedSecondaryCellTags*/,
        const TSecondaryMasterConnectionConfigs& /*reconfiguredSecondaryMasterConfigs*/,
        const THashSet<NObjectClient::TCellTag>& /*removedSecondaryTags*/);

    DECLARE_INTERFACE_SIGNAL(TCellReconfigurationSignature, CellDirectoryChanged);

    virtual void Update(const NCellMasterClient::NProto::TCellDirectory& protoDirectory) = 0;
    virtual void UpdateDefault() = 0;

    virtual NObjectClient::TCellId GetPrimaryMasterCellId() = 0;
    virtual NObjectClient::TCellTag GetPrimaryMasterCellTag() = 0;
    virtual NObjectClient::TCellTagList GetSecondaryMasterCellTags() = 0;
    virtual NObjectClient::TCellIdList GetSecondaryMasterCellIds() = 0;

    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(
        NApi::EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTagSentinel) = 0;
    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(
        NApi::EMasterChannelKind kind,
        NObjectClient::TCellId cellId) = 0;

    virtual NObjectClient::TCellTagList GetMasterCellTagsWithRole(EMasterCellRole role) = 0;

    virtual NObjectClient::TCellId GetRandomMasterCellWithRoleOrThrow(EMasterCellRole role) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellDirectory)

////////////////////////////////////////////////////////////////////////////////

ICellDirectoryPtr CreateCellDirectory(
    TCellDirectoryConfigPtr config,
    NApi::NNative::TConnectionOptions options,
    NRpc::IChannelFactoryPtr channelFactory,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient
