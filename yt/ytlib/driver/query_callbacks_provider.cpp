#include "query_callbacks_provider.h"
#include "private.h"

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <ytlib/new_table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/chunk_meta.pb.h>

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
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DriverLogger;

class TQueryCallbacksProvider::TImpl
    : public TRefCounted
    , public IPrepareCallbacks
{
public:
    TImpl(IChannelPtr masterChannel)
        : MasterChannel_(masterChannel)
        , ObjectProxy_(masterChannel) // TODO(sandello@): Configure proxy timeout.
    { }

    ~TImpl()
    { }

    virtual TFuture<TErrorOr<TDataSplit>> GetInitialSplit(const TYPath& path) override
    {
        return BIND(&TImpl::DoGetInitialSplit, MakeStrong(this), path)
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

private:
    TErrorOr<TDataSplit> DoGetInitialSplit(const Stroka& path)
    {
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

        if (attributes.Get<bool>("sorted")) {
            NTableClient::NProto::TKeyColumnsExt protoKeyColumns;
            ToProto(
                protoKeyColumns.mutable_values(),
                attributes.Get<std::vector<Stroka>>("sorted_by"));
            SetProtoExtension(result.mutable_extensions(), protoKeyColumns);
        }

        auto maybeTableSchema = attributes.Find<TTableSchema>("schema");
        if (!maybeTableSchema) {
            auto error = TError("Table %s is missing schema", ~path);
            LOG_DEBUG(error);
            return error;
        }

        NVersionedTableClient::NProto::TTableSchema protoTableSchema;
        ToProto(&protoTableSchema, *maybeTableSchema);
        SetProtoExtension(result.mutable_extensions(), protoTableSchema);

        LOG_DEBUG("Got attributes for table %s", ~path);

        return result;
    }

private:
    IChannelPtr MasterChannel_;
    TObjectServiceProxy ObjectProxy_;

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

