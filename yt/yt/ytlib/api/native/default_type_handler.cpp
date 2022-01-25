#include "default_type_handler.h"

#include "type_handler.h"
#include "client_impl.h"
#include "rpc_helpers.h"

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

namespace NYT::NApi::NNative {

using namespace NYson;
using namespace NYPath;
using namespace NYTree;
using namespace NRpc;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDefaultTypeHandler
    : public ITypeHandler
{
public:
    explicit TDefaultTypeHandler(TClient* client)
        : Client_(client)
    { }

    std::optional<TObjectId> CreateObject(
        EObjectType type,
        const TCreateObjectOptions& options) override
    {
        auto attributes = options.Attributes ? options.Attributes->Clone() : EmptyAttributes().Clone();
        return Client_->CreateObjectImpl(
            type,
            PrimaryMasterCellTagSentinel,
            *attributes,
            options);
    }

    std::optional<TYsonString> GetNode(
        const TYPath& path,
        const TGetNodeOptions& options) override
    {
        auto proxy = Client_->CreateReadProxy<TObjectServiceProxy>(options);
        auto batchReq = proxy->ExecuteBatch();
        batchReq->SetSuppressTransactionCoordinatorSync(options.SuppressTransactionCoordinatorSync);
        SetPrerequisites(batchReq, options);
        Client_->SetBalancingHeader(batchReq, options);

        auto req = TYPathProxy::Get(path);
        Client_->SetTransactionId(req, options, true);
        Client_->SetSuppressAccessTracking(req, options);
        Client_->SetCachingHeader(req, options);
        if (options.Attributes) {
            ToProto(req->mutable_attributes()->mutable_keys(), *options.Attributes);
        }
        if (options.MaxSize) {
            req->set_limit(*options.MaxSize);
        }
        if (options.Options) {
            ToProto(req->mutable_options(), *options.Options);
        }
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>(0)
            .ValueOrThrow();

        return TYsonString(rsp->value());
    }

    std::optional<TYsonString> ListNode(
        const TYPath& path,
        const TListNodeOptions& options) override
    {
        auto proxy = Client_->CreateReadProxy<TObjectServiceProxy>(options);
        auto batchReq = proxy->ExecuteBatch();
        batchReq->SetSuppressTransactionCoordinatorSync(options.SuppressTransactionCoordinatorSync);
        SetPrerequisites(batchReq, options);
        Client_->SetBalancingHeader(batchReq, options);

        auto req = TYPathProxy::List(path);
        Client_->SetTransactionId(req, options, true);
        Client_->SetSuppressAccessTracking(req, options);
        Client_->SetCachingHeader(req, options);
        if (options.Attributes) {
            ToProto(req->mutable_attributes()->mutable_keys(), *options.Attributes);
        }
        if (options.MaxSize) {
            req->set_limit(*options.MaxSize);
        }
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspList>(0)
            .ValueOrThrow();

        return TYsonString(rsp->value());
    }

private:
    TClient* const Client_;
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateDefaultTypeHandler(TClient* client)
{
    return New<TDefaultTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
