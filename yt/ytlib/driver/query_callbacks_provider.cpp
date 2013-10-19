#include "query_callbacks_provider.h"
#include "private.h"

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/table_client/table_ypath_proxy.h>

#include <ytlib/new_table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/chunk_meta.pb.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/query_client/stubs.h>

#include <core/ytree/ypath_proxy.h>
#include <core/ytree/attribute_helpers.h>

#include <core/concurrency/fiber.h>

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
    TImpl(IChannelPtr masterChannel)
        : MasterChannel_(masterChannel)
        , ObjectProxy_(masterChannel) // TODO(sandello@): Configure proxy timeout.
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
    TErrorOr<TDataSplit> DoGetInitialSplit(const Stroka& path)
    {
        typedef NTableClient::NProto::TKeyColumnsExt TProtoKeyColumns;
        typedef NVersionedTableClient::NProto::TTableSchemaExt TProtoTableSchema;

        LOG_DEBUG("Getting attributes for table %s", ~path);

        auto req = TYPathProxy::Get(path);
        // TODO(sandello): Wrap in transaction.
        TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
        attributeFilter.Keys.push_back("id");
        attributeFilter.Keys.push_back("sorted");
        attributeFilter.Keys.push_back("sorted_by");
        attributeFilter.Keys.push_back("schema");
        ToProto(req->mutable_attribute_filter(), attributeFilter);

        auto rsp = WaitFor(ObjectProxy_.Execute(req));
        if (!rsp->IsOK()) {
            auto error = TError("Error getting table attributes") << rsp->GetError();
            LOG_DEBUG(error);
            return error;
        }

        auto node = ConvertToNode(TYsonString(rsp->value()));
        const auto& attributes = node->Attributes();

        TDataSplit result;
        ToProto(
            result.mutable_chunk_id(),
            attributes.Get<TObjectId>("id"));

        TProtoKeyColumns protoKeyColumns;
        if (attributes.Get<bool>("sorted")) {
            ToProto(
                protoKeyColumns.mutable_values(),
                attributes.Get<std::vector<Stroka>>("sorted_by"));
        }
        SetProtoExtension(result.mutable_extensions(), protoKeyColumns);

        auto maybeTableSchema = attributes.Find<TProtoTableSchema>("schema");
        if (!maybeTableSchema) {
            auto error = TError("Table %s is missing schema", ~path);
            LOG_DEBUG(error);
            return error;
        }

        SetProtoExtension(result.mutable_extensions(), *maybeTableSchema);

        LOG_DEBUG("Got attributes for table %s", ~path);

        return result;
    }

    TErrorOr<std::vector<TDataSplit>> DoSplitFurther(const TDataSplit& split)
    {
        auto objectId = FromProto<TObjectId>(split.chunk_id());
        switch (TypeFromId(objectId)) {
            case NObjectClient::EObjectType::Table:
                return DoSplitTableFurther(split);
            default:
                YUNIMPLEMENTED();
        }
    }

    TErrorOr<std::vector<TDataSplit>> DoSplitTableFurther(const TDataSplit& split)
    {
        auto objectId = FromProto<TObjectId>(split.chunk_id());
        LOG_DEBUG("Splitting table further into chunks (ObjectId: %s)", ~ToString(objectId));

        typedef NTableClient::NProto::TKeyColumnsExt TProtoKeyColumns;
        typedef NVersionedTableClient::NProto::TTableSchemaExt TProtoTableSchema;

        auto path = FromObjectId(objectId);
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
    TNodeDirectoryPtr NodeDirectory_;

};

TQueryCallbacksProvider::TQueryCallbacksProvider(IChannelPtr masterChannel)
    : Impl_(New<TImpl>(std::move(masterChannel)))
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

