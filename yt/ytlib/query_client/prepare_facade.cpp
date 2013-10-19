#include "prepare_facade.h"
#include "private.h"

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <ytlib/new_table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/chunk_meta.pb.h>

#include <core/ytree/ypath_proxy.h>
#include <core/ytree/attribute_helpers.h>

#include <core/concurrency/fiber.h>

namespace NYT {
namespace NQueryClient {

using namespace NRpc;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NYPath;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = QueryClientLogger;

class TPrepareFacade::TImpl
{
public:
    TImpl(TIntrusivePtr<TPrepareFacade> self, IChannelPtr masterChannel)
        : Self_(std::move(self))
        , MasterChannel_(std::move(masterChannel))
        // TODO(sandello@): Configure proxy timeout.
        , ObjectProxy_(MasterChannel_)
    { }

    ~TImpl()
    { }

    TFuture<TErrorOr<TDataSplit>> GetInitialSplit(const TYPath& path)
    {
        auto self = Self_;
        return BIND([self, path, this] { return DoGetInitialSplit(path); })
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

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
    TIntrusivePtr<TPrepareFacade> Self_;

    IChannelPtr MasterChannel_;
    TObjectServiceProxy ObjectProxy_;
};

TPrepareFacade::TPrepareFacade(IChannelPtr masterChannel)
    : Impl_(new TImpl(this, std::move(masterChannel)))
{ }

TPrepareFacade::~TPrepareFacade()
{ }

TFuture<TErrorOr<TDataSplit>> TPrepareFacade::GetInitialSplit(
    const NYT::NYPath::TYPath& path
)
{
    return Impl_->GetInitialSplit(path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

