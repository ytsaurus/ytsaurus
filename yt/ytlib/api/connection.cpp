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

#include <ytlib/tablet_client/table_mount_cache.h>

#include <ytlib/query_client/callbacks.h>

namespace NYT {
namespace NApi {

using namespace NRpc;
using namespace NYPath;
using namespace NHive;
using namespace NHydra;
using namespace NChunkClient;
using namespace NTabletClient;
using namespace NQueryClient;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

class TPrepareCallbacks
    : public IPrepareCallbacks
{
public:
    explicit TPrepareCallbacks(IConnectionPtr connection)
        : Connection_(connection)
    { }

    virtual TFuture<TErrorOr<TDataSplit>> GetInitialSplit(const TYPath& path) override
    {
        YUNIMPLEMENTED();
    }

private:
    IConnectionPtr Connection_;

};

////////////////////////////////////////////////////////////////////////////////

class TCoordinateCallbacks
    : public ICoordinateCallbacks
{
public:
    explicit TCoordinateCallbacks(IConnectionPtr connection)
        : Connection_(connection)
    { }

    virtual ISchemedReaderPtr GetReader(const TDataSplit& dataSplit) override
    {
        YUNIMPLEMENTED();
    }

    virtual bool CanSplit(
        const TDataSplit& dataSplit) override
    {
        YUNIMPLEMENTED();
    }

    virtual TFuture<TErrorOr<std::vector<TDataSplit>>> SplitFurther(
        const TDataSplit& dataSplit) override
    {
        YUNIMPLEMENTED();
    }

    virtual ISchemedReaderPtr Delegate(
        const TPlanFragment& fragment,
        const TDataSplit& colocatedDataSplit) override
    {
        YUNIMPLEMENTED();
    }

private:
    IConnectionPtr Connection_;

};

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

        BlockCache_ = CreateClientBlockCache(
            Config_->BlockCache);

        TableMountCache_ = New<TTableMountCache>(
            Config_->TableMountCache,
            MasterChannel_,
            CellDirectory_);

        PrepareCallbacks_.reset(new TPrepareCallbacks(this));
        CoordinateCallbacks_.reset(new TCoordinateCallbacks(this));
    }


    virtual TConnectionConfigPtr GetConfig() override
    {
        return Config_;
    }

    virtual IChannelPtr GetMasterChannel() override
    {
        return MasterChannel_;
    }

    virtual IChannelPtr GetSchedulerChannel() override
    {
        return SchedulerChannel_;
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

    virtual IPrepareCallbacks* GetQueryPrepareCallbacks() override
    {
        return PrepareCallbacks_.get();
    }

    virtual ICoordinateCallbacks* GetQueryCoordinateCallbacks() override
    {
        return CoordinateCallbacks_.get();
    }

private:
    TConnectionConfigPtr Config_;

    IChannelPtr MasterChannel_;
    IChannelPtr SchedulerChannel_;
    IBlockCachePtr BlockCache_;
    TTableMountCachePtr TableMountCache_;
    ITimestampProviderPtr TimestampProvider_;
    TCellDirectoryPtr CellDirectory_;
    std::unique_ptr<TPrepareCallbacks> PrepareCallbacks_;
    std::unique_ptr<TCoordinateCallbacks> CoordinateCallbacks_;

};

IConnectionPtr CreateConnection(TConnectionConfigPtr config)
{
    return New<TConnection>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
