#include "admin.h"
#include "config.h"
#include "connection.h"
#include "private.h"

#include <yt/ytlib/chunk_client/client_block_cache.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/ytlib/hydra/peer_channel.h>

#include <yt/ytlib/query_client/column_evaluator.h>
#include <yt/ytlib/query_client/evaluator.h>
#include <yt/ytlib/query_client/functions_cache.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/scheduler/scheduler_channel.h>

#include <yt/ytlib/tablet_client/table_mount_cache.h>

#include <yt/ytlib/transaction_client/config.h>
#include <yt/ytlib/transaction_client/remote_timestamp_provider.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/rpc/bus_channel.h>
#include <yt/core/rpc/caching_channel_factory.h>
#include <yt/core/rpc/retrying_channel.h>

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

IAdminPtr CreateAdmin(IConnectionPtr connection, const TAdminOptions& options);
IClientPtr CreateClient(IConnectionPtr connection, const TClientOptions& options);

////////////////////////////////////////////////////////////////////////////////

class TConnection
    : public IConnection
{
public:
    TConnection(TConnectionConfigPtr config, const TConnectionOptions& options)
        : Config_(config)
        , Options_(options)
    { }

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

    virtual const TCellTagList& GetSecondaryMasterCellTags() const override
    {
        return SecondaryMasterCellTags_;
    }

    virtual IChannelPtr GetMasterChannelOrThrow(
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

    virtual IChannelFactoryPtr GetLightChannelFactory() override
    {
        return LightChannelFactory_;
    }

    virtual IChannelFactoryPtr GetHeavyChannelFactory() override
    {
        return HeavyChannelFactory_;
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

    virtual TEvaluatorPtr GetQueryEvaluator() override
    {
        return QueryEvaluator_;
    }

    virtual TColumnEvaluatorCachePtr GetColumnEvaluatorCache() override
    {
        return ColumnEvaluatorCache_;
    }

    virtual IInvokerPtr GetLightInvoker() override
    {
        return LightPool_->GetInvoker();
    }

    virtual IInvokerPtr GetHeavyInvoker() override
    {
        return HeavyPool_->GetInvoker();
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
    const TConnectionOptions Options_;

    TCellId PrimaryMasterCellId_;
    TCellTag PrimaryMasterCellTag_;
    TCellTagList SecondaryMasterCellTags_;

    TEnumIndexedVector<yhash_map<TCellTag, IChannelPtr>, EMasterChannelKind> MasterChannels_;
    IChannelPtr SchedulerChannel_;
    IChannelFactoryPtr LightChannelFactory_;
    IChannelFactoryPtr HeavyChannelFactory_;
    IBlockCachePtr BlockCache_;
    TTableMountCachePtr TableMountCache_;
    ITimestampProviderPtr TimestampProvider_;
    TCellDirectoryPtr CellDirectory_;
    TEvaluatorPtr QueryEvaluator_;
    TColumnEvaluatorCachePtr ColumnEvaluatorCache_;
    TThreadPoolPtr LightPool_;
    TThreadPoolPtr HeavyPool_;

    IChannelPtr CreatePeerChannel(TMasterConnectionConfigPtr config, EPeerKind kind)
    {
        auto channel = NHydra::CreatePeerChannel(
            config,
            GetBusChannelFactory(),
            kind);

        auto isRetryableError = BIND([options = Options_] (const TError& error) {
            if (options.RetryRequestQueueSizeLimitExceeded &&
                error.GetCode() == NSecurityClient::EErrorCode::RequestQueueSizeLimitExceeded)
            {
                return true;
            }

            return IsRetriableError(error);
        });
        channel = CreateRetryingChannel(config, channel, isRetryableError);

        channel = CreateDefaultTimeoutChannel(channel, config->RpcTimeout);

        return channel;
    }

    void Initialize()
    {
        LightPool_ = New<TThreadPool>(Config_->LightPoolSize, "ClientLight");
        HeavyPool_ = New<TThreadPool>(Config_->HeavyPoolSize, "ClientHeavy");

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
            MasterChannels_[channelKind][cellTag] = CreatePeerChannel(config, peerKind);
        };

        auto leaderPeerKind = EPeerKind::Leader;
        auto followerPeerKind = Config_->EnableReadFromFollowers ? EPeerKind::Follower : EPeerKind::Leader;
        auto leaderOrFollowerPeerKind = Config_->EnableReadFromFollowers
            ? (Config_->ForceReadFromFollowers ? EPeerKind::Follower : EPeerKind::LeaderOrFollower)
            : EPeerKind::Leader;

        auto initMasterChannels = [&] (const TMasterConnectionConfigPtr& config) {
            initMasterChannel(EMasterChannelKind::Leader, config, leaderPeerKind);
            initMasterChannel(EMasterChannelKind::Follower, config, followerPeerKind);
            initMasterChannel(EMasterChannelKind::LeaderOrFollower, config, leaderOrFollowerPeerKind);
        };

        initMasterChannels(Config_->PrimaryMaster);
        for (const auto& masterConfig : Config_->SecondaryMasters) {
            initMasterChannels(masterConfig);
        }

        // NB: Caching is only possible for the primary master.
        auto masterCacheConfig = Config_->MasterCache ? Config_->MasterCache : Config_->PrimaryMaster;
        initMasterChannel(EMasterChannelKind::Cache, masterCacheConfig, leaderOrFollowerPeerKind);

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
            GetMasterChannelOrThrow(EMasterChannelKind::Leader));

        LightChannelFactory_ = CreateCachingChannelFactory(GetBusChannelFactory());
        HeavyChannelFactory_ = CreateCachingChannelFactory(GetBusChannelFactory());

        CellDirectory_ = New<TCellDirectory>(
            Config_->CellDirectory,
            LightChannelFactory_,
            Config_->NetworkName);
        CellDirectory_->ReconfigureCell(Config_->PrimaryMaster);
        for (const auto& cellConfig : Config_->SecondaryMasters) {
            CellDirectory_->ReconfigureCell(cellConfig);
        }

        BlockCache_ = CreateClientBlockCache(
            Config_->BlockCache,
            EBlockType::CompressedData|EBlockType::UncompressedData);

        TableMountCache_ = New<TTableMountCache>(
            Config_->TableMountCache,
            GetMasterChannelOrThrow(EMasterChannelKind::Cache),
            CellDirectory_);

        QueryEvaluator_ = New<TEvaluator>(Config_->QueryEvaluator);
        ColumnEvaluatorCache_ = New<TColumnEvaluatorCache>(Config_->ColumnEvaluatorCache);
    }

    friend IConnectionPtr CreateConnection(
        TConnectionConfigPtr config,
        const TConnectionOptions& options);
};

IConnectionPtr CreateConnection(
    TConnectionConfigPtr config,
    const TConnectionOptions& options)
{
    auto connection = New<TConnection>(config, options);
    connection->Initialize();
    return connection;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
