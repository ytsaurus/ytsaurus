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
        auto initMasterChannel = [&] (EMasterChannelKind channelKind, TMasterConnectionConfigPtr config, EPeerKind peerKind) {
            MasterChannels_[channelKind] = CreatePeerChannel(
                config,
                isRetriableError,
                peerKind);
        };

        auto masterConfig = Config_->Master;
        auto masterCacheConfig = Config_->MasterCache ? Config_->MasterCache : Config_->Master;
        initMasterChannel(EMasterChannelKind::Leader, masterConfig, EPeerKind::Leader);
        initMasterChannel(EMasterChannelKind::Follower, masterConfig, Config_->EnableReadFromFollowers ? EPeerKind::Follower : EPeerKind::Leader);
        initMasterChannel(EMasterChannelKind::LeaderOrFollower, masterConfig, Config_->EnableReadFromFollowers ? EPeerKind::LeaderOrFollower : EPeerKind::Leader);
        initMasterChannel(EMasterChannelKind::Cache, masterCacheConfig, Config_->EnableReadFromFollowers ? EPeerKind::LeaderOrFollower : EPeerKind::Leader);

        auto timestampProviderConfig = Config_->TimestampProvider;
        if (!timestampProviderConfig) {
            // Use masters for timestamp generation.
            timestampProviderConfig = New<TRemoteTimestampProviderConfig>();
            timestampProviderConfig->Addresses = Config_->Master->Addresses;
            timestampProviderConfig->RpcTimeout = Config_->Master->RpcTimeout;
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
        CellDirectory_->ReconfigureCell(Config_->Master);

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

    virtual IChannelPtr GetMasterChannel(EMasterChannelKind kind) override
    {
        return MasterChannels_[kind];
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

    TEnumIndexedVector<IChannelPtr, EMasterChannelKind> MasterChannels_;
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
