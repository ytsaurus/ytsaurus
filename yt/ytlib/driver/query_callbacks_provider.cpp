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

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/query_client/stubs.h>

namespace NYT {
namespace NDriver {

using namespace NRpc;
using namespace NYPath;
using namespace NYTree;
using namespace NConcurrency;
using namespace NCypressClient;
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

    virtual TFuture<TErrorOr<TDataSplit>> GetInitialSplit(const TYPath& path) override
    {
        return BIND(&TImpl::DoGetInitialSplit, MakeStrong(this), path)
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    virtual TFuture<TErrorOr<std::vector<TDataSplit>>> SplitFurther(const TDataSplit& split) override
    {
        return BIND(&TImpl::DoSplitFurther, MakeStrong(this), split)
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    virtual IExecutorPtr GetColocatedExecutor(const TDataSplit& split) override
    {
        return nullptr;
    }

    virtual IExecutorPtr GetLocalExecutor() override
    {
        return nullptr;
    }

private:
    TErrorOr<TDataSplit> DoGetInitialSplit(const TYPath& path)
    {
        typedef NTableClient::NProto::TKeyColumnsExt TProtoKeyColumns;
        typedef NVersionedTableClient::NProto::TTableSchemaExt TProtoTableSchema;

        auto asyncInfoOrError = TableMountCache_->LookupInfo(path);
        auto infoOrError = WaitFor(asyncInfoOrError);
        THROW_ERROR_EXCEPTION_IF_FAILED(infoOrError);
        const auto& info = infoOrError.GetValue();

        TDataSplit result;

        ToProto(result.mutable_chunk_id(), info->TableId); /// TODO(babenko): WTF? table_id != chunk_id!

        TProtoKeyColumns protoKeyColumns;
        ToProto(protoKeyColumns.mutable_names(), info->KeyColumns);
        SetProtoExtension(result.mutable_extensions(), protoKeyColumns);

        SetProtoExtension(result.mutable_extensions(), NYT::ToProto<NVersionedTableClient::NProto::TTableSchemaExt>(info->Schema));

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

    TErrorOr<std::vector<TDataSplit>> DoSplitTableFurther(const TDataSplit& split)
    {
        // TODO(babenko): caching?

        auto tableId = FromProto<TObjectId>(split.chunk_id());
        LOG_DEBUG("Splitting table further into chunks (TableId: %s)",
            ~ToString(tableId));

        typedef NTableClient::NProto::TKeyColumnsExt TProtoKeyColumns;
        typedef NVersionedTableClient::NProto::TTableSchemaExt TProtoTableSchema;

        auto path = FromObjectId(tableId);
        auto req = TTableYPathProxy::Fetch(path);
        auto rsp = WaitFor(ObjectProxy_.Execute(req));

        if (!rsp->IsOK()) {
            auto error = TError("Error fetching table info") << rsp->GetError();
            LOG_DEBUG(error);
            return error;
        }

        NodeDirectory_->MergeFrom(rsp->node_directory());
        auto chunkSpecs = FromProto<NChunkClient::NProto::TChunkSpec>(rsp->chunks());

        auto originalKeyColumns = GetProtoExtension<TProtoKeyColumns>(split.extensions());
        auto originalTableSchema = GetProtoExtension<TProtoTableSchema>(split.extensions());

        for (auto& chunkSpec : chunkSpecs) {
            // TODO(babenko): why not GetProtoExtension?
            auto keyColumns = FindProtoExtension<TProtoKeyColumns>(chunkSpec.extensions());
            auto tableSchema = FindProtoExtension<TProtoTableSchema>(chunkSpec.extensions());
            // TODO(sandello): One day we should validate consistency.
            YCHECK(!keyColumns);
            YCHECK(!tableSchema);

            SetProtoExtension(chunkSpec.mutable_extensions(), originalKeyColumns);
            SetProtoExtension(chunkSpec.mutable_extensions(), originalTableSchema);
        }

        return chunkSpecs;
    }

private:
    IChannelPtr MasterChannel_;
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

