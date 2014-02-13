#include "stdafx.h"
#include "connection.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/fiber.h>

#include <core/rpc/bus_channel.h>
#include <core/rpc/caching_channel_factory.h>

#include <ytlib/hydra/peer_channel.h>

#include <ytlib/scheduler/scheduler_channel.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_client/chunk_replica.h>

#include <ytlib/hive/cell_directory.h>
#include <ytlib/hive/timestamp_provider.h>
#include <ytlib/hive/remote_timestamp_provider.h>

#include <ytlib/tablet_client/table_mount_cache.h>

#include <ytlib/query_client/callbacks.h>
#include <ytlib/query_client/helpers.h>
#include <ytlib/query_client/plan_context.h>
#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/query_service_proxy.h>

#include <ytlib/driver/dispatcher.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <util/random/random.h>

// TODO(babenko): killme
#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/table_client/table_ypath_proxy.h>
#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client//chunk_meta_extensions.h>
#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/new_table_client/schemed_reader.h>
#include <ytlib/new_table_client/schemed_chunk_reader.h>

namespace NYT {
namespace NApi {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYPath;
using namespace NHive;
using namespace NHydra;
using namespace NChunkClient;
using namespace NTabletClient;
using namespace NQueryClient;
using namespace NVersionedTableClient;
using namespace NObjectClient;
using namespace NTableClient;  // TODO(babenko): killme
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = ApiLogger;

////////////////////////////////////////////////////////////////////////////////

class TRemoteReader
    : public NChunkClient::IAsyncReader
{
public:
    explicit TRemoteReader(TFuture<TQueryServiceProxy::TRspExecutePtr> response)
        : Response_(std::move(response))
    { }

    virtual TAsyncReadResult AsyncReadBlocks(const std::vector<int>& blockIndexes) override
    {
        return Response_.Apply(BIND(
            &TRemoteReader::ReadBlocks,
            blockIndexes));
    }

    virtual TAsyncGetMetaResult AsyncGetChunkMeta(
        const TNullable<int>& partitionTag = Null,
        const std::vector<int>* tags = nullptr) override
    {
        return Response_.Apply(BIND(
            &TRemoteReader::GetChunkMeta,
            partitionTag,
            MakeNullable(tags)));
    }

    virtual TChunkId GetChunkId() const override
    {
        return NullChunkId;
    }

private:
    TFuture<TQueryServiceProxy::TRspExecutePtr> Response_;

    static TReadResult ReadBlocks(
        const std::vector<int>& blockIndexes,
        TQueryServiceProxy::TRspExecutePtr rsp)
    {
        if (!rsp->IsOK()) {
            return rsp->GetError();
        }
        std::vector<TSharedRef> blocks;
        for (auto index : blockIndexes) {
            YCHECK(index < rsp->Attachments().size());
            blocks.push_back(rsp->Attachments()[index]);
        }
        return std::move(blocks);
    }

    static TGetMetaResult GetChunkMeta(
        const TNullable<int>& partitionTag,
        const TNullable<std::vector<int>> extensionTags,
        TQueryServiceProxy::TRspExecutePtr rsp)
    {
        if (!rsp->IsOK()) {
            return rsp->GetError();
        }
        return rsp->chunk_meta();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TConnection
    : public IConnection
    , public IPrepareCallbacks
    , public ICoordinateCallbacks
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

        NodeChannelFactory_ = CreateCachingChannelFactory(GetBusChannelFactory());

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

    virtual IPrepareCallbacks* GetQueryPrepareCallbacks() override
    {
        return this;
    }

    virtual ICoordinateCallbacks* GetQueryCoordinateCallbacks() override
    {
        return this;
    }


    // IPrepareCallbacks implementation.

    virtual TFuture<TErrorOr<TDataSplit>> GetInitialSplit(
        const TYPath& path,
        TPlanContextPtr context) override
    {
        return BIND(&TConnection::DoGetInitialSplit, MakeStrong(this))
            .Guarded()
            .AsyncVia(NDriver::TDispatcher::Get()->GetLightInvoker())
            .Run(path, std::move(context));
    }


    // ICoordinateCallbacks implementation.
    
    virtual ISchemedReaderPtr GetReader(
        const TDataSplit& /*split*/,
        TPlanContextPtr /*context*/) override
    {
        YUNREACHABLE();
    }

    virtual bool CanSplit(const TDataSplit& split) override
    {
        auto objectId = GetObjectIdFromDataSplit(split);
        auto type = TypeFromId(objectId);
        return type == EObjectType::Table;
    }

    virtual TFuture<TErrorOr<std::vector<TDataSplit>>> SplitFurther(
        const TDataSplit& split,
        TPlanContextPtr context) override
    {
        return
            BIND(&TConnection::DoSplitFurther, MakeStrong(this))
                .Guarded()
                .AsyncVia(NDriver::TDispatcher::Get()->GetLightInvoker())
                .Run(split, std::move(context));
    }

    virtual ISchemedReaderPtr Delegate(
        const TPlanFragment& fragment,
        const TDataSplit& colocatedSplit) override
    {
        auto replicas = FromProto<TChunkReplica, TChunkReplicaList>(colocatedSplit.replicas());
        if (replicas.empty()) {
            THROW_ERROR_EXCEPTION("No alive replicas for split %s",
                ~ToString(GetObjectIdFromDataSplit(colocatedSplit)));
        }

        auto replica = replicas[RandomNumber(replicas.size())];

        auto nodeDirectory = fragment.GetContext()->GetNodeDirectory();
        auto& nodeDescriptor = nodeDirectory->GetDescriptor(replica);

        LOG_DEBUG("Delegating fragment (FragmentId: %s, Address: %s)",
            ~ToString(fragment.Id()),
            ~nodeDescriptor.Address);

        auto channel = NodeChannelFactory_->CreateChannel(nodeDescriptor.Address);

        TQueryServiceProxy proxy(channel);
        auto req = proxy.Execute();
        // TODO(sandello): Send only relevant part of nodeDirectory.
        nodeDirectory->DumpTo(req->mutable_node_directory());
        ToProto(req->mutable_plan_fragment(), fragment);

        return CreateSchemedChunkReader(
            New<TChunkReaderConfig>(),
            New<TRemoteReader>(req->Invoke()));
    }

private:
    typedef NTableClient::NProto::TOldBoundaryKeysExt TProtoBoundaryKeys;
    typedef NTableClient::NProto::TKeyColumnsExt TProtoKeyColumns;
    typedef NVersionedTableClient::NProto::TTableSchemaExt TProtoTableSchema;

    TConnectionConfigPtr Config_;

    IChannelPtr MasterChannel_;
    IChannelPtr SchedulerChannel_;
    IChannelFactoryPtr NodeChannelFactory_;
    IBlockCachePtr BlockCache_;
    TTableMountCachePtr TableMountCache_;
    ITimestampProviderPtr TimestampProvider_;
    TCellDirectoryPtr CellDirectory_;



    TDataSplit DoGetInitialSplit(
        const TYPath& path,
        TPlanContextPtr /*context*/)
    {
        LOG_DEBUG("Getting initial split (Path: %s)",
            ~path);

        auto asyncInfoOrError = TableMountCache_->LookupTableInfo(path);
        auto infoOrError = WaitFor(asyncInfoOrError);
        THROW_ERROR_EXCEPTION_IF_FAILED(infoOrError);
        const auto& info = infoOrError.GetValue();

        TDataSplit result;
        SetObjectId(&result, info->TableId);
        SetTableSchema(&result, info->Schema);
        SetKeyColumns(&result, info->KeyColumns);
        return result;
    }


    std::vector<TDataSplit> DoSplitFurther(
        const TDataSplit& split,
        TPlanContextPtr context)
    {
        auto objectId = GetObjectIdFromDataSplit(split);

        std::vector<TDataSplit> subsplits;
        switch (TypeFromId(objectId)) {
            case EObjectType::Table:
                subsplits = DoSplitTableFurther(split, std::move(context));
                break;

            default:
                YUNREACHABLE();
        }

        LOG_DEBUG("Subsplits built (ObjectId: %s, SubsplitCount: %d)",
            ~ToString(objectId),
            static_cast<int>(subsplits.size()));

        return subsplits;
    }

    std::vector<TDataSplit> DoSplitTableFurther(
        const TDataSplit& split,
        TPlanContextPtr context)
    {
        auto tableId = GetObjectIdFromDataSplit(split);
        auto tableInfoOrError = WaitFor(TableMountCache_->LookupTableInfo(FromObjectId(tableId)));
        THROW_ERROR_EXCEPTION_IF_FAILED(tableInfoOrError);
        const auto& tableInfo = tableInfoOrError.GetValue();

        return tableInfo->Sorted
            ? DoSplitSortedTableFurther(split, std::move(context))
            : DoSplitUnsortedTableFurther(split, std::move(context), std::move(tableInfo));
    }

    std::vector<TDataSplit> DoSplitSortedTableFurther(
        const TDataSplit& split,
        TPlanContextPtr context)
    {
        auto tableId = GetObjectIdFromDataSplit(split);
        LOG_DEBUG("Splitting sorted table further into chunks (TableId: %s)",
            ~ToString(tableId));

        // TODO(babenko): refactor and optimize
        TObjectServiceProxy proxy(MasterChannel_);

        auto req = TTableYPathProxy::Fetch(FromObjectId(tableId));
        req->set_fetch_all_meta_extensions(true);

        auto rsp = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

        context->GetNodeDirectory()->MergeFrom(rsp->node_directory());

        auto chunkSpecs = FromProto<NChunkClient::NProto::TChunkSpec>(rsp->chunks());
        auto keyColumns = FromProto<Stroka>(GetProtoExtension<TProtoKeyColumns>(split.chunk_meta().extensions()).names());
        auto schema = FromProto<TTableSchema>(GetProtoExtension<TProtoTableSchema>(split.chunk_meta().extensions()));

        for (auto& chunkSpec : chunkSpecs) {
            auto chunkKeyColumns = FindProtoExtension<TProtoKeyColumns>(chunkSpec.chunk_meta().extensions());
            auto chunkSchema = FindProtoExtension<TProtoTableSchema>(chunkSpec.chunk_meta().extensions());

            // TODO(sandello): One day we should validate consistency.
            // Now we just check we do _not_ have any of these.
            YCHECK(!chunkKeyColumns);
            YCHECK(!chunkSchema);

            SetKeyColumns(&chunkSpec, keyColumns);
            SetTableSchema(&chunkSpec, schema);

            auto boundaryKeys = FindProtoExtension<TProtoBoundaryKeys>(chunkSpec.chunk_meta().extensions());
            if (boundaryKeys) {
                auto chunkLowerBound = NYT::FromProto<TOwningKey>(boundaryKeys->start());
                auto chunkUpperBound = NYT::FromProto<TOwningKey>(boundaryKeys->end());
                // Boundary keys are exact, so advance right bound to its successor.
                chunkUpperBound = GetKeySuccessor(chunkUpperBound.Get());
                SetLowerBound(&chunkSpec, chunkLowerBound);
                SetUpperBound(&chunkSpec, chunkUpperBound);
            }
        }

        return chunkSpecs;
    }

    std::vector<TDataSplit> DoSplitUnsortedTableFurther(
        const TDataSplit& split,
        TPlanContextPtr context,
        TTableMountInfoPtr tableInfo)
    {
        auto tableId = GetObjectIdFromDataSplit(split);
        LOG_DEBUG("Splitting unsorted table further into tablets (TableId: %s)",
            ~ToString(tableId));

        if (tableInfo->Tablets.empty()) {
            THROW_ERROR_EXCEPTION("Table %s is neither sorted nor has tablets",
                ~ToString(tableId));
        }

        auto lowerBound = GetLowerBoundFromDataSplit(split);
        auto upperBound = GetUpperBoundFromDataSplit(split);

        // Run binary search to find the relevant tablets.
        auto lowerIt = std::upper_bound(
            tableInfo->Tablets.begin(),
            tableInfo->Tablets.end(),
            lowerBound,
            [] (const TOwningKey& key, const TTabletInfoPtr& tabletInfo) {
                return key < tabletInfo->PivotKey;
            }) - 1;

        auto nodeDirectory = context->GetNodeDirectory();

        auto keyColumns = FromProto<Stroka>(GetProtoExtension<TProtoKeyColumns>(split.chunk_meta().extensions()).names());
        auto schema = FromProto<TTableSchema>(GetProtoExtension<TProtoTableSchema>(split.chunk_meta().extensions()));

        std::vector<TDataSplit> subsplits;
        for (auto it = lowerIt; it != tableInfo->Tablets.end(); ++it) {
            const auto& tabletInfo = *it;
            if (upperBound <= tabletInfo->PivotKey)
                break;

            if (tabletInfo->State != ETabletState::Mounted) {
                // TODO(babenko): learn to work with unmounted tablets
                THROW_ERROR_EXCEPTION("Tablet %s is not mounted",
                    ~ToString(tabletInfo->TabletId));
            }

            TDataSplit subsplit;
            SetObjectId(&subsplit, tabletInfo->TabletId);   
            SetKeyColumns(&subsplit, keyColumns);
            SetTableSchema(&subsplit, schema);
            
            SetLowerBound(&subsplit, tabletInfo->PivotKey);
            auto jt = it + 1;
            SetUpperBound(&subsplit, jt == tableInfo->Tablets.end() ? MaxKey() : (*jt)->PivotKey);

            for (const auto& tabletReplica : tabletInfo->Replicas) {
                nodeDirectory->AddDescriptor(tabletReplica.Id, tabletReplica.Descriptor);
                TChunkReplica chunkReplica(tabletReplica.Id, 0);
                subsplit.add_replicas(ToProto<ui32>(chunkReplica));
            }

            subsplits.push_back(subsplit);
        }
        return subsplits;
    }

};

IConnectionPtr CreateConnection(TConnectionConfigPtr config)
{
    return New<TConnection>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
