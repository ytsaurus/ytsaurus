#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/public.h>

#include <yt/yt/client/cell_master_client/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NCellMasterClient {

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateMasterCacheChannel(
    const NApi::NNative::TMasterConnectionConfigPtr& config,
    const NApi::NNative::TMasterConnectionConfigPtr& masterCacheConfig,
    const NRpc::IChannelFactoryPtr channelFactory,
    const NApi::NNative::TConnectionOptions& options,
    const std::vector<std::string>& discoveredAddresses);

////////////////////////////////////////////////////////////////////////////////

struct ICellDirectory
    : public virtual TRefCounted
{
    using TCellReconfigurationSignature = void(
        const TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs,
        const TSecondaryMasterConnectionConfigs& changedSecondaryMasterConfigs,
        const THashSet<NObjectClient::TCellTag>& removedSecondaryMasterCellTags);
    DECLARE_INTERFACE_SIGNAL(TCellReconfigurationSignature, CellDirectoryChanged);

    virtual void Update(
        const NCellMasterClient::NProto::TCellDirectory& protoDirectory,
        // NB: Used for testing purposes only.
        bool duplicate) = 0;
    virtual void UpdateDefault() = 0;

    virtual NObjectClient::TCellId GetPrimaryMasterCellId() = 0;
    virtual NObjectClient::TCellTag GetPrimaryMasterCellTag() = 0;
    virtual NObjectClient::TCellTagList GetSecondaryMasterCellTags() = 0;
    virtual THashSet<NObjectClient::TCellId> GetSecondaryMasterCellIds() = 0;

    virtual bool IsClientSideCacheEnabled() const = 0;
    virtual bool IsMasterCacheEnabled() const = 0;

    virtual NRpc::IChannelPtr FindMasterChannel(
        NApi::EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTagSentinel) = 0;
    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(
        NApi::EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTagSentinel) = 0;
    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(
        NApi::EMasterChannelKind kind,
        NObjectClient::TCellId cellId) = 0;

    // NB: naked master cell channels don't have neither retries nor default
    // timeouts.
    virtual NRpc::IChannelPtr FindNakedMasterChannel(
        NApi::EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTagSentinel) = 0;
    virtual NRpc::IChannelPtr GetNakedMasterChannelOrThrow(
        NApi::EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTagSentinel) = 0;

    //! Throws when passed EMasterCellRole::Unknown. Returns empty list if no cells have the specified role.
    virtual NObjectClient::TCellTagList GetMasterCellTagsWithRole(EMasterCellRole role) = 0;

    //! Throws when passed EMasterCellRole::Unknown. Throws if no cells have the specified role.
    virtual NObjectClient::TCellId GetRandomMasterCellWithRoleOrThrow(EMasterCellRole role) = 0;

    //! Returns secondary masters connection configuration.
    virtual TSecondaryMasterConnectionConfigs GetSecondaryMasterConnectionConfigs() = 0;

    //! Reconfigures master connection directory.
    virtual void ReconfigureMasterCellDirectory(
        const TSecondaryMasterConnectionConfigs& secondaryMasterConnectionConfigs) = 0;

    // TODO(cherepashka): make it static function or just separate from ICellDirectory.
    virtual bool ClusterMasterCompositionChanged(
        const TSecondaryMasterConnectionConfigs& oldSecondaryMasterConnectionConfigs,
        const TSecondaryMasterConnectionConfigs& newSecondaryMasterConnectionConfigs) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellDirectory)

////////////////////////////////////////////////////////////////////////////////

ICellDirectoryPtr CreateCellDirectory(
    TCellDirectoryConfigPtr config,
    NApi::NNative::TConnectionOptions options,
    NRpc::IChannelFactoryPtr channelFactory,
    TWeakPtr<NApi::NNative::IConnection> connection,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

// TODO(cherepashka): move into helpers file.
NObjectClient::TCellTagList GetMasterCellTags(const TSecondaryMasterConnectionConfigs& masterConnectionConfigs);
THashSet<NObjectClient::TCellId> GetMasterCellIds(const TSecondaryMasterConnectionConfigs& masterConnectionConfigs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient
