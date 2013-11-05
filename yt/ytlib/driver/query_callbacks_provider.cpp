#include "query_callbacks_provider.h"
#include "table_mount_cache.h"
#include "private.h"

#include <core/misc/protobuf_helpers.h>

#include <core/ytree/ypath_proxy.h>
#include <core/ytree/attribute_helpers.h>

#include <core/concurrency/fiber.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/table_client/table_ypath_proxy.h>

#include <ytlib/new_table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/chunk_meta.pb.h>
#include <ytlib/new_table_client/reader.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/query_client/callbacks.h>
#include <ytlib/query_client/executor.h> // For DelegateToPeer
#include <ytlib/query_client/helpers.h>

#include <core/ytree/ypath_proxy.h>
#include <core/ytree/attribute_helpers.h>

#include <core/rpc/channel_cache.h>

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

    virtual TFuture<TErrorOr<std::vector<TDataSplit>>> SplitFurther(
        const TDataSplit& split) override
    {
        return BIND(&TImpl::DoSplitFurther, MakeStrong(this), split)
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    virtual IReaderPtr Delegate(
        const TQueryFragment& fragment,
        const TDataSplit& colocatedDataSplit) override
    {
        auto replicas = NYT::FromProto<TChunkReplica, TChunkReplicaList>(colocatedDataSplit.replicas());
        auto replica = replicas[RandomNumber(replicas.size())];

        auto& descriptor = NodeDirectory_->GetDescriptor(replica);
        IChannelPtr channel;

        try {
            LOG_DEBUG("Opening a channel to %s", ~descriptor.Address);
            channel = NodeChannelCache_.GetChannel(descriptor.Address);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                "Failed to open a channel to %s",
                ~descriptor.Address) << ex;
        }

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
            case NObjectClient::EObjectType::Chunk:
                return std::vector<TDataSplit>(1, split);
            default:
                YUNIMPLEMENTED();
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
        auto originalKeyColumns = GetProtoExtension<TProtoKeyColumns>(dataSplit.extensions());
        auto originalTableSchema = GetProtoExtension<TProtoTableSchema>(dataSplit.extensions());

        for (auto& chunkSpec : chunkSpecs) {
            auto keyColumns = FindProtoExtension<TProtoKeyColumns>(chunkSpec.extensions());
            auto tableSchema = FindProtoExtension<TProtoTableSchema>(chunkSpec.extensions());
            // TODO(sandello): One day we should validate consistency.
            // Now we just check we do _not_ have any of these.
            YCHECK(!keyColumns);
            YCHECK(!tableSchema);

            SetProtoExtension(chunkSpec.mutable_extensions(), originalKeyColumns);
            SetProtoExtension(chunkSpec.mutable_extensions(), originalTableSchema);
        }

        return chunkSpecs;
    }

private:
    IChannelPtr MasterChannel_;
    TChannelCache NodeChannelCache_;
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

