#include "default_type_handler.h"

#include "type_handler_detail.h"
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
using namespace NTabletClient;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDefaultTypeHandler
    : public TNullTypeHandler
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

    std::optional<TNodeId> CreateNode(
        EObjectType type,
        const TYPath& path,
        const TCreateNodeOptions& options) override
    {
        auto attributes = options.Attributes ? options.Attributes->Clone() : EmptyAttributes().Clone();
        return Client_->CreateNodeImpl(type, path, *attributes, options);
    }

    void SetComplexityLimits(TYPathRequest& req, const auto& options)
    {
        auto& header = req.Header();
        auto* ypathExt = header.MutableExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
        ToProto(ypathExt->mutable_read_complexity_limits(), options.ComplexityLimits);
    }

    std::optional<TYsonString> GetNode(
        const TYPath& path,
        const TGetNodeOptions& options) override
    {
        auto proxy = Client_->CreateObjectServiceReadProxy(options);
        auto batchReq = proxy.ExecuteBatch();
        SetSuppressUpstreamSyncs(batchReq, options);
        SetPrerequisites(batchReq, options);
        Client_->SetBalancingHeader(batchReq, options);

        auto req = TYPathProxy::Get(path);
        Client_->SetTransactionId(req, options, true);
        Client_->SetSuppressAccessTracking(req, options);
        Client_->SetCachingHeader(req, options);
        if (options.Attributes) {
            ToProto(req->mutable_attributes(), options.Attributes);
        }
        if (options.MaxSize) {
            req->set_limit(*options.MaxSize);
        }
        SetComplexityLimits(*req, options);
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
        auto proxy = Client_->CreateObjectServiceReadProxy(options);
        auto batchReq = proxy.ExecuteBatch();
        SetSuppressUpstreamSyncs(batchReq, options);
        SetPrerequisites(batchReq, options);
        Client_->SetBalancingHeader(batchReq, options);

        auto req = TYPathProxy::List(path);
        Client_->SetTransactionId(req, options, true);
        Client_->SetSuppressAccessTracking(req, options);
        Client_->SetCachingHeader(req, options);
        if (options.Attributes) {
            ToProto(req->mutable_attributes(), options.Attributes);
        }
        if (options.MaxSize) {
            req->set_limit(*options.MaxSize);
        }
        SetComplexityLimits(*req, options);
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspList>(0)
            .ValueOrThrow();

        return TYsonString(rsp->value());
    }

    std::optional<bool> NodeExists(
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options) override
    {
        auto proxy = Client_->CreateObjectServiceReadProxy(options);
        auto batchReq = proxy.ExecuteBatch();
        SetSuppressUpstreamSyncs(batchReq, options);
        SetPrerequisites(batchReq, options);
        Client_->SetBalancingHeader(batchReq, options);

        auto req = TYPathProxy::Exists(path);
        Client_->SetTransactionId(req, options, true);
        Client_->SetSuppressAccessTracking(req, options);
        Client_->SetCachingHeader(req, options);
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspExists>(0)
            .ValueOrThrow();

        return rsp->value();
    }

    std::optional<std::monostate> RemoveNode(
        const TYPath& path,
        const TRemoveNodeOptions& options) override
    {
        auto proxy = Client_->CreateObjectServiceWriteProxy();
        auto batchReq = proxy.ExecuteBatch();
        SetSuppressUpstreamSyncs(batchReq, options);
        SetPrerequisites(batchReq, options);

        auto req = TYPathProxy::Remove(path);
        Client_->SetTransactionId(req, options, true);
        Client_->SetMutationId(req, options);
        req->set_recursive(options.Recursive);
        req->set_force(options.Force);
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        batchRsp->GetResponse<TYPathProxy::TRspRemove>(0)
            .ThrowOnError();

        return std::monostate();
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
