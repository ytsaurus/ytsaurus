#include "admin.h"
#include "config.h"
#include "connection.h"
#include "client.h"
#include "admin.h"
#include "transaction_participant.h"
#include "transaction.h"
#include "private.h"

#include <yt/ytlib/chunk_client/client_block_cache.h>

#include <yt/ytlib/hive/cell_directory.h>
#include <yt/ytlib/hive/cell_directory_synchronizer.h>
#include <yt/ytlib/hive/cluster_directory.h>
#include <yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/ytlib/hydra/peer_channel.h>

#include <yt/ytlib/query_client/column_evaluator.h>
#include <yt/ytlib/query_client/evaluator.h>
#include <yt/ytlib/query_client/functions_cache.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/scheduler/scheduler_channel.h>

#include <yt/client/tablet_client/table_mount_cache.h>
#include <yt/ytlib/tablet_client/native_table_mount_cache.h>

#include <yt/ytlib/transaction_client/config.h>
#include <yt/client/transaction_client/remote_timestamp_provider.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/lease_manager.h>

#include <yt/core/rpc/bus/channel.h>
#include <yt/core/rpc/caching_channel_factory.h>
#include <yt/core/rpc/retrying_channel.h>

namespace NYT {
namespace NApi {
namespace NNative {

using namespace NConcurrency;
using namespace NRpc;
using namespace NHiveClient;
using namespace NChunkClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

class TConnection
    : public IConnection
{
public:
    TConnection(
        TConnectionConfigPtr config,
        const TConnectionOptions& options)
        : Config_(config)
        , Options_(options)
        , Logger(NLogging::TLogger(ApiLogger)
            .AddTag("PrimaryCellTag: %v, ConnectionId: %",
                CellTagFromId(Config_->PrimaryMaster->CellId),
                TGuid::Create()))
        , ChannelFactory_(CreateCachingChannelFactory(NRpc::NBus::CreateBusChannelFactory(Config_->BusClient)))
    { }

    void Initialize()
    {
        if (Config_->ThreadPoolSize) {
            ThreadPool_ = New<TThreadPool>(*Config_->ThreadPoolSize, "Connection");
        }

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

        auto initMasterChannels = [&] (const TMasterConnectionConfigPtr& config) {
            initMasterChannel(EMasterChannelKind::Leader, config, leaderPeerKind);
            initMasterChannel(EMasterChannelKind::Follower, config, followerPeerKind);
        };

        initMasterChannels(Config_->PrimaryMaster);
        for (const auto& masterConfig : Config_->SecondaryMasters) {
            initMasterChannels(masterConfig);
        }

        // NB: Caching is only possible for the primary master.
        auto masterCacheConfig = Config_->MasterCache ? Config_->MasterCache : Config_->PrimaryMaster;
        initMasterChannel(EMasterChannelKind::Cache, masterCacheConfig, followerPeerKind);

        auto timestampProviderConfig = Config_->TimestampProvider;
        if (!timestampProviderConfig) {
            // Use masters for timestamp generation.
            timestampProviderConfig = New<TRemoteTimestampProviderConfig>();
            timestampProviderConfig->Addresses = Config_->PrimaryMaster->Addresses;
            timestampProviderConfig->RpcTimeout = Config_->PrimaryMaster->RpcTimeout;
        }
        TimestampProvider_ = CreateRemoteTimestampProvider(
            timestampProviderConfig,
            ChannelFactory_);

        SchedulerChannel_ = CreateSchedulerChannel(
            Config_->Scheduler,
            ChannelFactory_,
            GetMasterChannelOrThrow(EMasterChannelKind::Leader),
            GetNetworks());

        ClusterDirectory_ = New<TClusterDirectory>();
        ClusterDirectorySynchronizer_ = New<TClusterDirectorySynchronizer>(
            Config_->ClusterDirectorySynchronizer,
            this,
            ClusterDirectory_);

        CellDirectory_ = New<TCellDirectory>(
            Config_->CellDirectory,
            ChannelFactory_,
            GetNetworks(),
            Logger);
        CellDirectory_->ReconfigureCell(Config_->PrimaryMaster);
        for (const auto& cellConfig : Config_->SecondaryMasters) {
            CellDirectory_->ReconfigureCell(cellConfig);
        }
        CellDirectorySynchronizer_ = New<TCellDirectorySynchronizer>(
            Config_->CellDirectorySynchronizer,
            CellDirectory_,
            PrimaryMasterCellId_,
            Logger);

        BlockCache_ = CreateClientBlockCache(
            Config_->BlockCache,
            EBlockType::CompressedData|EBlockType::UncompressedData);

        TableMountCache_ = CreateNativeTableMountCache(
            Config_->TableMountCache,
            GetMasterChannelOrThrow(EMasterChannelKind::Cache),
            CellDirectory_,
            Logger);

        QueryEvaluator_ = New<TEvaluator>(Config_->QueryEvaluator);
        ColumnEvaluatorCache_ = New<TColumnEvaluatorCache>(Config_->ColumnEvaluatorCache);
    }

    // IConnection implementation.

    virtual TCellTag GetCellTag() override
    {
        return CellTagFromId(Config_->PrimaryMaster->CellId);
    }

    virtual const ITableMountCachePtr& GetTableMountCache() override
    {
        return TableMountCache_;
    }

    virtual const ITimestampProviderPtr& GetTimestampProvider() override
    {
        return TimestampProvider_;
    }

    virtual IInvokerPtr GetInvoker() override
    {
        return ThreadPool_ ? ThreadPool_->GetInvoker() : GetCurrentInvoker();
    }

    virtual IAdminPtr CreateAdmin(const TAdminOptions& options) override
    {
        return NNative::CreateAdmin(this, options);
    }

    virtual NApi::IClientPtr CreateClient(const TClientOptions& options) override
    {
        return NNative::CreateClient(this, options);
    }

    virtual void ClearMetadataCaches() override
    {
        TableMountCache_->Clear();
    }

    // NNative::IConnection implementation.

    virtual const TConnectionConfigPtr& GetConfig() override
    {
        return Config_;
    }

    virtual const TNetworkPreferenceList& GetNetworks() const override
    {
        return Config_->Networks.Get(DefaultNetworkPreferences);
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

    virtual IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        const TCellId& cellId) override
    {
        if (ReplaceCellTagInId(cellId, 0) != ReplaceCellTagInId(PrimaryMasterCellId_, 0)) {
            THROW_ERROR_EXCEPTION("Unknown master cell id %v",
                cellId);
        }
        return GetMasterChannelOrThrow(kind, CellTagFromId(cellId));
    }

    virtual const IChannelPtr& GetSchedulerChannel() override
    {
        return SchedulerChannel_;
    }

    virtual const IChannelFactoryPtr& GetChannelFactory() override
    {
        return ChannelFactory_;
    }

    virtual const IBlockCachePtr& GetBlockCache() override
    {
        return BlockCache_;
    }

    virtual const TEvaluatorPtr& GetQueryEvaluator() override
    {
        return QueryEvaluator_;
    }

    virtual const TColumnEvaluatorCachePtr& GetColumnEvaluatorCache() override
    {
        return ColumnEvaluatorCache_;
    }


    virtual const TCellDirectoryPtr& GetCellDirectory() override
    {
        return CellDirectory_;
    }

    virtual const TCellDirectorySynchronizerPtr& GetCellDirectorySynchronizer() override
    {
        return CellDirectorySynchronizer_;
    }


    virtual const TClusterDirectoryPtr& GetClusterDirectory() override
    {
        return ClusterDirectory_;
    }

    virtual const TClusterDirectorySynchronizerPtr& GetClusterDirectorySynchronizer() override
    {
        return ClusterDirectorySynchronizer_;
    }


    virtual IClientPtr CreateNativeClient(const TClientOptions& options) override
    {
        return NNative::CreateClient(this, options);
    }

    virtual NHiveClient::ITransactionParticipantPtr CreateTransactionParticipant(
        const TCellId& cellId,
        const TTransactionParticipantOptions& options) override
    {
        // For tablet writes, manual sync is not needed since Table Mount Cache
        // is responsible for populating cell directory. Transaction participants,
        // on the other hand, have no other way to keep cell directory up-to-date.
        CellDirectorySynchronizer_->Start();
        return NNative::CreateTransactionParticipant(
            CellDirectory_,
            CellDirectorySynchronizer_,
            TimestampProvider_,
            this,
            cellId,
            options);
    }

    virtual ITransactionPtr RegisterStickyTransaction(NNative::ITransactionPtr transaction) override
    {
        const auto& transactionId = transaction->GetId();
        TStickyTransactionEntry entry{
            transaction,
            TLeaseManager::CreateLease(
                transaction->GetTimeout(),
                BIND(&TConnection::OnStickyTransactionLeaseExpired, MakeWeak(this), transactionId))
        };

        {
            TWriterGuard guard(StickyTransactionLock_);
            YCHECK(IdToStickyTransactionEntry_.emplace(transactionId, entry).second);
        }

        transaction->SubscribeCommitted(BIND(&TConnection::OnStickyTransactionFinished, MakeWeak(this), transactionId));
        transaction->SubscribeAborted(BIND(&TConnection::OnStickyTransactionFinished, MakeWeak(this), transactionId));

        LOG_DEBUG("Sticky transaction registered (TransactionId: %v)",
            transactionId);

        return transaction;
    }

    virtual ITransactionPtr GetStickyTransaction(const TTransactionId& transactionId) override
    {
        ITransactionPtr transaction;
        TLease lease;
        {
            TReaderGuard guard(StickyTransactionLock_);
            auto it = IdToStickyTransactionEntry_.find(transactionId);
            if (it == IdToStickyTransactionEntry_.end()) {
                THROW_ERROR_EXCEPTION(
                    NTransactionClient::EErrorCode::NoSuchTransaction,
                    "Sticky transaction %v is not found",
                    transactionId);
            }
            const auto& entry = it->second;
            transaction = entry.Transaction;
            lease = entry.Lease;
        }
        TLeaseManager::RenewLease(lease);
        LOG_DEBUG("Sticky transaction lease renewed (TransactionId: %v)",
            transactionId);
        return transaction;
    }

    virtual void Terminate() override
    {
        Terminated_ = true;

        ClusterDirectory_->Clear();
        ClusterDirectorySynchronizer_->Stop();

        CellDirectory_->Clear();
        CellDirectorySynchronizer_->Stop();
    }

    virtual bool IsTerminated() override
    {
        return Terminated_;
    }

private:
    const TConnectionConfigPtr Config_;
    const TConnectionOptions Options_;

    const NLogging::TLogger Logger;

    const NRpc::IChannelFactoryPtr ChannelFactory_;

    TCellId PrimaryMasterCellId_;
    TCellTag PrimaryMasterCellTag_;
    TCellTagList SecondaryMasterCellTags_;

    TEnumIndexedVector<THashMap<TCellTag, IChannelPtr>, EMasterChannelKind> MasterChannels_;
    IChannelPtr SchedulerChannel_;
    IBlockCachePtr BlockCache_;
    ITableMountCachePtr TableMountCache_;
    ITimestampProviderPtr TimestampProvider_;
    TEvaluatorPtr QueryEvaluator_;
    TColumnEvaluatorCachePtr ColumnEvaluatorCache_;

    TCellDirectoryPtr CellDirectory_;
    TCellDirectorySynchronizerPtr CellDirectorySynchronizer_;

    TClusterDirectoryPtr ClusterDirectory_;
    TClusterDirectorySynchronizerPtr ClusterDirectorySynchronizer_;

    TThreadPoolPtr ThreadPool_;

    std::atomic<bool> Terminated_ = {false};

    struct TStickyTransactionEntry
    {
        ITransactionPtr Transaction;
        TLease Lease;
    };

    TReaderWriterSpinLock StickyTransactionLock_;
    THashMap<TTransactionId, TStickyTransactionEntry> IdToStickyTransactionEntry_;


    IChannelPtr CreatePeerChannel(const TMasterConnectionConfigPtr& config, EPeerKind kind)
    {
        auto channel = NHydra::CreatePeerChannel(
            config,
            ChannelFactory_,
            kind);

        auto isRetryableError = BIND([options = Options_] (const TError& error) {
            if (error.FindMatching(NChunkClient::EErrorCode::OptimisticLockFailure)) {
                return true;
            }

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


    void OnStickyTransactionLeaseExpired(const TTransactionId& transactionId)
    {
        ITransactionPtr transaction;
        {
            TWriterGuard guard(StickyTransactionLock_);
            auto it = IdToStickyTransactionEntry_.find(transactionId);
            if (it == IdToStickyTransactionEntry_.end()) {
                return;
            }
            transaction = it->second.Transaction;
            IdToStickyTransactionEntry_.erase(it);
        }

        LOG_DEBUG("Sticky transaction lease expired (TransactionId: %v)",
            transactionId);

        transaction->Abort();
    }

    void OnStickyTransactionFinished(const TTransactionId& transactionId)
    {
        TLease lease;
        {
            TWriterGuard guard(StickyTransactionLock_);
            auto it = IdToStickyTransactionEntry_.find(transactionId);
            if (it == IdToStickyTransactionEntry_.end()) {
                return;
            }
            lease = it->second.Lease;
            IdToStickyTransactionEntry_.erase(it);
        }

        LOG_DEBUG("Sticky transaction unregistered (TransactionId: %v)",
            transactionId);

        TLeaseManager::CloseLease(lease);
    }
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

} // namespace NNative
} // namespace NApi
} // namespace NYT
