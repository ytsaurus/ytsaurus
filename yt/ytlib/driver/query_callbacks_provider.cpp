#include "query_callbacks_provider.h"
#include "table_mount_cache.h"
#include "private.h"

#include <core/misc/protobuf_helpers.h>

#include <core/ytree/ypath_proxy.h>
#include <core/ytree/attribute_helpers.h>

#include <core/concurrency/fiber.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/table_client/table_ypath_proxy.h>

#include <ytlib/new_table_client/chunk_reader.h>
#include <ytlib/new_table_client/chunk_writer.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/chunk_meta.pb.h>
#include <ytlib/new_table_client/reader.h>

#include <ytlib/chunk_client/async_reader.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/query_client/callbacks.h>
#include <ytlib/query_client/helpers.h>
#include <ytlib/query_client/query_service_proxy.h>
#include <ytlib/query_client/plan_fragment.h>

#include <core/ytree/ypath_proxy.h>
#include <core/ytree/attribute_helpers.h>

#include <core/rpc/caching_channel_factory.h>

#include <core/concurrency/fiber.h>

#include <util/random/random.h>

namespace NYT {
namespace NDriver {

using namespace NRpc;
using namespace NYPath;
using namespace NYTree;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NVersionedTableClient;
using namespace NQueryClient;
using namespace NNodeTrackerClient;

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

IReaderPtr DelegateToPeer(
    const TPlanFragment& planFragment,
    TNodeDirectoryPtr nodeDirectory,
    IChannelPtr channel)
{
    TQueryServiceProxy proxy(channel);

    auto req = proxy.Execute();

    nodeDirectory->DumpTo(req->mutable_node_directory());
    ToProto(req->mutable_plan_fragment(), planFragment);

    return CreateChunkReader(
        New<TChunkReaderConfig>(),
        New<TRemoteReader>(req->Invoke()));
}

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DriverLogger;

class TQueryCallbacksProvider::TImpl
    : public TRefCounted
    , public IPrepareCallbacks
    , public ICoordinateCallbacks
{
public:
    TImpl(
        IChannelPtr masterChannel,
        TTableMountCachePtr tableMountCache)
        : MasterChannel_(masterChannel)
        , ObjectProxy_(masterChannel)
        , TableMountCache_(std::move(tableMountCache))
        , NodeDirectory_(New<TNodeDirectory>())
    { }

    ~TImpl()
    { }

    virtual TFuture<TErrorOr<TDataSplit>> GetInitialSplit(const TYPath& path) override
    {
        return BIND(&TImpl::DoGetInitialSplit, MakeStrong(this), path)
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    virtual bool CanSplit(const TDataSplit& dataSplit) override
    {
        auto objectId = GetObjectIdFromDataSplit(dataSplit);
        switch (TypeFromId(objectId)) {
            case NObjectClient::EObjectType::Table:
                return true;
            default:
                return false;
        }
    }

    virtual TFuture<TErrorOr<std::vector<TDataSplit>>> SplitFurther(
        const TDataSplit& dataSplit) override
    {
        // TODO(sandello): Is it safe to pass const reference here?
        return BIND(&TImpl::DoSplitFurther, MakeStrong(this), ConstRef(dataSplit))
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    virtual IReaderPtr Delegate(
        const TPlanFragment& fragment,
        const TDataSplit& colocatedDataSplit) override
    {
        auto replicas = NYT::FromProto<TChunkReplica, TChunkReplicaList>(colocatedDataSplit.replicas());
        auto replica = replicas[RandomNumber(replicas.size())];

        auto& descriptor = NodeDirectory_->GetDescriptor(replica);

        LOG_DEBUG("Opening a channel to %s", ~descriptor.Address);
        auto channel = ChannelFactory_->CreateChannel(descriptor.Address);

        // TODO(sandello): Send only relevant part of NodeDirectory_.
        return DelegateToPeer(fragment, NodeDirectory_, channel);
    }

    virtual IReaderPtr GetReader(const TDataSplit& dataSplit) override
    {
        YUNREACHABLE();
    }

private:
    TErrorOr<TDataSplit> DoGetInitialSplit(const TYPath& path)
    {
        LOG_DEBUG("Getting info for table %s", ~path);

        auto asyncInfoOrError = TableMountCache_->LookupInfo(path);
        auto infoOrError = WaitFor(asyncInfoOrError);
        THROW_ERROR_EXCEPTION_IF_FAILED(infoOrError);
        const auto& info = infoOrError.GetValue();

        TDataSplit result;

        SetObjectId(&result, info->TableId);
        SetTableSchema(&result, info->Schema); // info->TableSchema?
        SetKeyColumns(&result, info->KeyColumns);

        LOG_DEBUG("Got info for table %s", ~path);

        return result;
    }

    TErrorOr<std::vector<TDataSplit>> DoSplitFurther(const TDataSplit& split)
    {
        auto objectId = FromProto<TObjectId>(split.chunk_id());
        LOG_DEBUG("Trying to split data (ObjectId: %s, ObjectType: %s)",
            ~ToString(objectId),
            ~TypeFromId(objectId).ToString());

        switch (TypeFromId(objectId)) {
            case NObjectClient::EObjectType::Table:
                return DoSplitTableFurther(split);
            default:
                YUNREACHABLE();
        }
    }

    TErrorOr<std::vector<TDataSplit>> DoSplitTableFurther(const TDataSplit& dataSplit)
    {
        auto objectId = GetObjectIdFromDataSplit(dataSplit);
        LOG_DEBUG("Splitting table further into chunks (TableId: %s)",
            ~ToString(objectId));

        auto path = FromObjectId(objectId);
        auto req = TTableYPathProxy::Fetch(path);
        req->set_fetch_all_meta_extensions(true);
        auto rsp = WaitFor(ObjectProxy_.Execute(req));

        if (!rsp->IsOK()) {
            auto error = TError("Error fetching table info") << rsp->GetError();
            LOG_DEBUG(error);
            return error;
        }

        NodeDirectory_->MergeFrom(rsp->node_directory());
        auto chunkSpecs = FromProto<NChunkClient::NProto::TChunkSpec>(rsp->chunks());

        typedef NTableClient::NProto::TKeyColumnsExt TProtoKeyColumns;
        typedef NVersionedTableClient::NProto::TTableSchemaExt TProtoTableSchema;
        auto originalKeyColumns = GetProtoExtension<TProtoKeyColumns>(dataSplit.chunk_meta().extensions());
        auto originalTableSchema = GetProtoExtension<TProtoTableSchema>(dataSplit.chunk_meta().extensions());

        for (auto& chunkSpec : chunkSpecs) {
            auto keyColumns = FindProtoExtension<TProtoKeyColumns>(chunkSpec.chunk_meta().extensions());
            auto tableSchema = FindProtoExtension<TProtoTableSchema>(chunkSpec.chunk_meta().extensions());
            // TODO(sandello): One day we should validate consistency.
            // Now we just check we do _not_ have any of these.
            YCHECK(!keyColumns);
            YCHECK(!tableSchema);

            SetProtoExtension(chunkSpec.mutable_chunk_meta()->mutable_extensions(), originalKeyColumns);
            SetProtoExtension(chunkSpec.mutable_chunk_meta()->mutable_extensions(), originalTableSchema);
        }

        return chunkSpecs;
    }

private:
    IChannelPtr MasterChannel_;
    IChannelFactoryPtr ChannelFactory_;
    TObjectServiceProxy ObjectProxy_;
    TTableMountCachePtr TableMountCache_;
    TNodeDirectoryPtr NodeDirectory_;

};

TQueryCallbacksProvider::TQueryCallbacksProvider(
    IChannelPtr masterChannel,
    TTableMountCachePtr tableMountCache)
    : Impl_(New<TImpl>(
        std::move(masterChannel),
        std::move(tableMountCache)))
{ }

TQueryCallbacksProvider::~TQueryCallbacksProvider()
{ }

NQueryClient::IPrepareCallbacks* TQueryCallbacksProvider::GetPrepareCallbacks()
{
    return Impl_.Get();
}

NQueryClient::ICoordinateCallbacks* TQueryCallbacksProvider::GetCoordinateCallbacks()
{
    return Impl_.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

