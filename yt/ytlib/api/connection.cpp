#include "stdafx.h"
#include "connection.h"
#include "admin.h"
#include "config.h"
#include "private.h"

#include <core/rpc/bus_channel.h>
#include <core/rpc/retrying_channel.h>
#include <core/rpc/caching_channel_factory.h>

#include <ytlib/hydra/peer_channel.h>

#include <ytlib/scheduler/scheduler_channel.h>

#include <ytlib/chunk_client/client_block_cache.h>

#include <ytlib/hive/cell_directory.h>

#include <ytlib/tablet_client/table_mount_cache.h>

#include <ytlib/transaction_client/remote_timestamp_provider.h>
#include <ytlib/transaction_client/config.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/query_client/evaluator.h>
#include <ytlib/query_client/column_evaluator.h>

namespace NYT {
namespace NApi {

using namespace NConcurrency;
using namespace NRpc;
using namespace NHive;
using namespace NChunkClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TClientOptions GetRootClientOptions()
{
    TClientOptions options;
    options.User = NSecurityClient::RootUserName;
    return options;
}

////////////////////////////////////////////////////////////////////////////////

IAdminPtr CreateAdmin(IConnectionPtr connection, const TAdminOptions& options);
IClientPtr CreateClient(IConnectionPtr connection, const TClientOptions& options);

////////////////////////////////////////////////////////////////////////////////

class TConnection
    : public IConnection
{
public:
    explicit TConnection(TConnectionConfigPtr config)
        : Config_(config)
    { }

    void Initialize(TCallback<bool(const TError&)> isRetriableError)
    {
        PrimaryMasterCellId_ = Config_->PrimaryMaster->CellId;
        PrimaryMasterCellTag_ = CellTagFromId(PrimaryMasterCellId_);
        for (const auto& masterConfig : Config_->SecondaryMasters) {
            SecondaryMasterCellTags_.push_back(CellTagFromId(masterConfig->CellId));
        }

        auto initMasterChannel = [&] (
            EMasterChannelKind channelKind,
            const TMasterConnectionConfigPtr& config,
            EPeerKind peerKind)
        {
            auto cellTag = CellTagFromId(config->CellId);
            MasterChannels_[channelKind][cellTag] = CreatePeerChannel(
                config,
                isRetriableError,
                peerKind);
        };
        auto initMasterChannels = [&] (const TMasterConnectionConfigPtr& config) {
            initMasterChannel(EMasterChannelKind::Leader, config, EPeerKind::Leader);
            initMasterChannel(EMasterChannelKind::Follower, config, Config_->EnableReadFromFollowers ? EPeerKind::Follower : EPeerKind::Leader);
            initMasterChannel(EMasterChannelKind::LeaderOrFollower, config, Config_->EnableReadFromFollowers ? EPeerKind::LeaderOrFollower : EPeerKind::Leader);
        };
        initMasterChannels(Config_->PrimaryMaster);
        for (const auto& masterConfig : Config_->SecondaryMasters) {
            initMasterChannels(masterConfig);
        }

        // NB: Caching is only possible for the primary master.
        auto masterCacheConfig = Config_->MasterCache ? Config_->MasterCache : Config_->PrimaryMaster;
        initMasterChannel(EMasterChannelKind::Cache, masterCacheConfig, Config_->EnableReadFromFollowers ? EPeerKind::LeaderOrFollower : EPeerKind::Leader);

        auto timestampProviderConfig = Config_->TimestampProvider;
        if (!timestampProviderConfig) {
            // Use masters for timestamp generation.
            timestampProviderConfig = New<TRemoteTimestampProviderConfig>();
            timestampProviderConfig->Addresses = Config_->PrimaryMaster->Addresses;
            timestampProviderConfig->RpcTimeout = Config_->PrimaryMaster->RpcTimeout;
        }
        TimestampProvider_ = CreateRemoteTimestampProvider(
            timestampProviderConfig,
            GetBusChannelFactory());

        SchedulerChannel_ = CreateSchedulerChannel(
            Config_->Scheduler,
            GetBusChannelFactory(),
            GetMasterChannel(EMasterChannelKind::Leader));

        NodeChannelFactory_ = CreateCachingChannelFactory(GetBusChannelFactory());

        CellDirectory_ = New<TCellDirectory>(
            Config_->CellDirectory,
            GetBusChannelFactory(),
            Config_->NetworkName);
        CellDirectory_->ReconfigureCell(Config_->PrimaryMaster);

        BlockCache_ = CreateClientBlockCache(
            Config_->BlockCache,
            EBlockType::CompressedData|EBlockType::UncompressedData);

        TableMountCache_ = New<TTableMountCache>(
            Config_->TableMountCache,
            GetMasterChannel(EMasterChannelKind::Cache),
            CellDirectory_);

        FunctionRegistry_ = CreateClientFunctionRegistry(
            CreateClient(TClientOptions()));

        QueryEvaluator_ = New<TEvaluator>(Config_->QueryEvaluator);
        ColumnEvaluatorCache_ = New<TColumnEvaluatorCache>(
            Config_->ColumnEvaluatorCache,
            FunctionRegistry_);
    }

    // IConnection implementation.

    virtual TConnectionConfigPtr GetConfig() override
    {
        return Config_;
    }

    virtual const TCellId& GetPrimaryMasterCellId() const override
    {
        return PrimaryMasterCellId_;
    }

    virtual TCellTag GetPrimaryMasterCellTag() const override
    {
        return PrimaryMasterCellTag_;
    }

    virtual const std::vector<TCellTag>& GetSecondaryMasterCellTags() const override
    {
        return SecondaryMasterCellTags_;
    }

    virtual IChannelPtr GetMasterChannel(
        EMasterChannelKind kind,
        TCellTag cellTag = PrimaryMasterCellTag) override
    {
        const auto& channels = MasterChannels_[kind];
        auto it = channels.find(cellTag == PrimaryMasterCellTag ? PrimaryMasterCellTag_ : cellTag);
        if (it == channels.end()) {
            THROW_ERROR_EXCEPTION("Unknown master cell tag %v",
                cellTag);
        }
        return it->second;
    }

    virtual IChannelPtr GetSchedulerChannel() override
    {
        return SchedulerChannel_;
    }

    virtual IChannelFactoryPtr GetNodeChannelFactory() override
    {
        return NodeChannelFactory_;
    }

    virtual IBlockCachePtr GetBlockCache() override
    {
        return BlockCache_;
    }

    virtual TTableMountCachePtr GetTableMountCache() override
    {
        return TableMountCache_;
    }

    virtual ITimestampProviderPtr GetTimestampProvider() override
    {
        return TimestampProvider_;
    }

    virtual TCellDirectoryPtr GetCellDirectory() override
    {
        return CellDirectory_;
    }

    virtual IFunctionRegistryPtr GetFunctionRegistry() override
    {
        return FunctionRegistry_;
    }

    virtual TEvaluatorPtr GetQueryEvaluator() override
    {
        return QueryEvaluator_;
    }

    virtual TColumnEvaluatorCachePtr GetColumnEvaluatorCache() override
    {
        return ColumnEvaluatorCache_;
    }

    virtual IAdminPtr CreateAdmin(const TAdminOptions& options) override
    {
        return NApi::CreateAdmin(this, options);
    }

    virtual IClientPtr CreateClient(const TClientOptions& options) override
    {
        return NApi::CreateClient(this, options);
    }

    virtual void ClearMetadataCaches() override
    {
        TableMountCache_->Clear();
    }


private:
    const TConnectionConfigPtr Config_;

    TCellId PrimaryMasterCellId_;
    TCellTag PrimaryMasterCellTag_;
    std::vector<TCellTag> SecondaryMasterCellTags_;

    TEnumIndexedVector<yhash_map<TCellTag, IChannelPtr>, EMasterChannelKind> MasterChannels_;
    IChannelPtr SchedulerChannel_;
    IChannelFactoryPtr NodeChannelFactory_;
    IBlockCachePtr BlockCache_;
    TTableMountCachePtr TableMountCache_;
    ITimestampProviderPtr TimestampProvider_;
    TCellDirectoryPtr CellDirectory_;
    IFunctionRegistryPtr FunctionRegistry_;
    TEvaluatorPtr QueryEvaluator_;
    TColumnEvaluatorCachePtr ColumnEvaluatorCache_;


    static IChannelPtr CreatePeerChannel(
        TMasterConnectionConfigPtr config,
        TCallback<bool(const TError&)> isRetriableError,
        EPeerKind kind)
    {
        auto channel = NHydra::CreatePeerChannel(
            config,
            GetBusChannelFactory(),
            kind);
        auto retryingChannel = CreateRetryingChannel(
            config,
            channel,
            isRetriableError);
        retryingChannel->SetDefaultTimeout(config->RpcTimeout);
        return retryingChannel;
    }

};

IConnectionPtr CreateConnection(
    TConnectionConfigPtr config,
    TCallback<bool(const TError&)> isRetriableError)
{
    auto connection = New<TConnection>(config);
    connection->Initialize(isRetriableError);
    return connection;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
