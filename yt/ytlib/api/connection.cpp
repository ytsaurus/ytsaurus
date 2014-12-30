#include "stdafx.h"
#include "connection.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/scheduler.h>

#include <core/rpc/bus_channel.h>
#include <core/rpc/retrying_channel.h>
#include <core/rpc/caching_channel_factory.h>

#include <core/compression/helpers.h>

#include <ytlib/hydra/peer_channel.h>
#include <ytlib/hydra/config.h>

#include <ytlib/scheduler/scheduler_channel.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/client_block_cache.h>

#include <ytlib/hive/cell_directory.h>

#include <ytlib/tablet_client/table_mount_cache.h>

#include <ytlib/transaction_client/timestamp_provider.h>
#include <ytlib/transaction_client/remote_timestamp_provider.h>
#include <ytlib/transaction_client/config.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/query_client/evaluator.h>

namespace NYT {
namespace NApi {

using namespace NConcurrency;
using namespace NRpc;
using namespace NHive;
using namespace NChunkClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

TClientOptions GetRootClientOptions()
{
    TClientOptions options;
    options.User = NSecurityClient::RootUserName;
    return options;
}

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(IConnectionPtr connection, const TClientOptions& options);

////////////////////////////////////////////////////////////////////////////////

class TConnection
    : public IConnection
{
public:
    explicit TConnection(
        TConnectionConfigPtr config,
        TCallback<bool(const TError&)> isRetriableError)
        : Config_(config)
    {
        MasterChannel_ = CreateMasterChannel(Config_->Master, isRetriableError);

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

        auto masterCacheConfig = Config_->MasterCache;
        if (!masterCacheConfig) {
            // Disable cache.
            masterCacheConfig = Config_->Master;
        }
        MasterCacheChannel_ = CreateMasterChannel(masterCacheConfig, isRetriableError);

        SchedulerChannel_ = CreateSchedulerChannel(
            Config_->Scheduler,
            GetBusChannelFactory(),
            MasterChannel_);

        NodeChannelFactory_ = CreateCachingChannelFactory(GetBusChannelFactory());

        CellDirectory_ = New<TCellDirectory>(
            Config_->CellDirectory,
            GetBusChannelFactory());
        CellDirectory_->RegisterCell(config->Master);

        CompressedBlockCache_ = CreateClientBlockCache(
            Config_->CompressedBlockCache);

        UncompressedBlockCache_ = CreateClientBlockCache(
            Config_->UncompressedBlockCache);

        TableMountCache_ = New<TTableMountCache>(
            Config_->TableMountCache,
            MasterCacheChannel_,
            CellDirectory_);

        QueryEvaluator_ = New<TEvaluator>(Config_->QueryEvaluator);
    }

    // IConnection implementation.

    virtual TConnectionConfigPtr GetConfig() override
    {
        return Config_;
    }

    virtual IChannelPtr GetMasterChannel() override
    {
        return MasterChannel_;
    }

    virtual IChannelPtr GetMasterCacheChannel() override
    {
        return MasterCacheChannel_;
    }

    virtual IChannelPtr GetSchedulerChannel() override
    {
        return SchedulerChannel_;
    }

    virtual IChannelFactoryPtr GetNodeChannelFactory() override
    {
        return NodeChannelFactory_;
    }

    virtual IBlockCachePtr GetCompressedBlockCache() override
    {
        return CompressedBlockCache_;
    }

    virtual IBlockCachePtr GetUncompressedBlockCache() override
    {
        return UncompressedBlockCache_;
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

    virtual IClientPtr CreateClient(const TClientOptions& options) override
    {
        return NApi::CreateClient(this, options);
    }

    virtual void ClearMetadataCaches() override
    {
        TableMountCache_->Clear();
    }


private:
    TConnectionConfigPtr Config_;

    IChannelPtr MasterChannel_;
    IChannelPtr MasterCacheChannel_;
    IChannelPtr SchedulerChannel_;
    IChannelFactoryPtr NodeChannelFactory_;
    IBlockCachePtr CompressedBlockCache_;
    IBlockCachePtr UncompressedBlockCache_;
    TTableMountCachePtr TableMountCache_;
    ITimestampProviderPtr TimestampProvider_;
    TCellDirectoryPtr CellDirectory_;
    TEvaluatorPtr QueryEvaluator_;

    
    static IChannelPtr CreateMasterChannel(
        TMasterConnectionConfigPtr config,
        TCallback<bool(const TError&)> isRetriableError)
    {
        auto leaderChannel = CreateLeaderChannel(
            config,
            GetBusChannelFactory());
        auto masterChannel = CreateRetryingChannel(
            config,
            leaderChannel,
            isRetriableError);
        masterChannel->SetDefaultTimeout(config->RpcTimeout);
        return masterChannel;
    }

};

IConnectionPtr CreateConnection(
    TConnectionConfigPtr config,
    TCallback<bool(const TError&)> isRetriableError)
{
    return New<TConnection>(config, isRetriableError);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
