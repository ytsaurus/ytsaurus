#include "stdafx.h"
#include "connection.h"
#include "config.h"

#include <core/rpc/bus_channel.h>

#include <ytlib/hydra/peer_channel.h>

#include <ytlib/scheduler/scheduler_channel.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/client_block_cache.h>

#include <ytlib/hive/cell_directory.h>
#include <ytlib/hive/timestamp_provider.h>
#include <ytlib/hive/remote_timestamp_provider.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/tablet_client/table_mount_cache.h>

namespace NYT {
namespace NApi {

using namespace NRpc;
using namespace NHive;
using namespace NHydra;
using namespace NChunkClient;
using namespace NTransactionClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

class TConnection
    : public IConnection
{
public:
    explicit TConnection(TConnectionConfigPtr config)
        : Config_(config)
    {
        auto channelFactory = GetBusChannelFactory();

        MasterChannel_ = CreatePeerChannel(
            Config_->Masters,
            channelFactory,
            EPeerRole::Leader);

        SchedulerChannel_ = CreateSchedulerChannel(
            Config_->Scheduler,
            channelFactory,
            MasterChannel_);

        TimestampProvider_ = CreateRemoteTimestampProvider(
            Config_->TimestampProvider,
            channelFactory);

        CellDirectory_ = New<TCellDirectory>(
            Config_->CellDirectory,
            channelFactory);
        CellDirectory_->RegisterCell(config->Masters);

        TransactionManager_ = New<TTransactionManager>(
            Config_->TransactionManager,
            Config_->Masters->CellGuid,
            MasterChannel_,
            TimestampProvider_,
            CellDirectory_);

        BlockCache_ = CreateClientBlockCache(
            Config_->BlockCache);

        TableMountCache_ = New<TTableMountCache>(
            Config_->TableMountCache,
            MasterChannel_,
            CellDirectory_);
    }


    virtual IChannelPtr GetMasterChannel() override
    {
        return MasterChannel_;
    }

    virtual IChannelPtr GetSchedulerChannel() override
    {
        return SchedulerChannel_;
    }

    virtual TTransactionManagerPtr GetTransactionManager() override
    {
        return TransactionManager_;
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

private:
    TConnectionConfigPtr Config_;

    IChannelPtr MasterChannel_;
    IChannelPtr SchedulerChannel_;
    TTransactionManagerPtr TransactionManager_;
    IBlockCachePtr BlockCache_;
    TTableMountCachePtr TableMountCache_;
    ITimestampProviderPtr TimestampProvider_;
    TCellDirectoryPtr CellDirectory_;

};

IConnectionPtr CreateConnection(TConnectionConfigPtr config)
{
    return New<TConnection>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
