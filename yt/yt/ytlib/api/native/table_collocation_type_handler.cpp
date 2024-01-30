#include "table_collocation_type_handler.h"

#include "type_handler_detail.h"
#include "client_impl.h"

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

namespace NYT::NApi::NNative {

using namespace NYson;
using namespace NYTree;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NCypressClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TTableCollocationTypeHandler
    : public TNullTypeHandler
{
public:
    explicit TTableCollocationTypeHandler(TClient* client)
        : Client_(client)
    { }

    std::optional<TObjectId> CreateObject(
        EObjectType type,
        const TCreateObjectOptions& options) override
    {
        if (type != EObjectType::TableCollocation) {
            return {};
        }

        auto attributes = options.Attributes ? options.Attributes->Clone() : EmptyAttributes().Clone();

        if (attributes->Contains("table_paths")) {
            if (attributes->Contains("table_ids")) {
                THROW_ERROR_EXCEPTION("Cannot specify both \"table_ids\" and \"table_paths\"");
            }

            auto tablePaths = attributes->GetAndRemove<std::vector<TYPath>>("table_paths");

            auto proxy = Client_->CreateObjectServiceReadProxy(TMasterReadOptions());
            auto batchReq = proxy.ExecuteBatch();
            for (const auto& tablePath : tablePaths) {
                auto req = TCypressYPathProxy::Get(tablePath + "/@id");
                batchReq->AddRequest(req);
            }
            auto batchRsp = WaitFor(batchReq->Invoke())
                .ValueOrThrow();

            std::vector<TTableId> tableIds;
            tableIds.reserve(tablePaths.size());
            for (const auto& rspOrError : batchRsp->GetResponses<TCypressYPathProxy::TRspGet>()) {
                const auto& rsp = rspOrError.ValueOrThrow();
                tableIds.push_back(ConvertTo<TTableId>(TYsonString(rsp->value())));
            }

            attributes->Set("table_ids", tableIds);
        }

        return Client_->CreateObjectImpl(
            type,
            PrimaryMasterCellTagSentinel,
            *attributes,
            options);
    }

private:
    TClient* const Client_;
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateTableCollocationTypeHandler(TClient* client)
{
    return New<TTableCollocationTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
