#include "sequoia_service.h"

#include "private.h"

#include "bootstrap.h"
#include "helpers.h"
#include "node_proxy.h"
#include "path_resolver.h"
#include "rootstock_proxy.h"

#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/sequoia_client/client.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NCypressProxy {

using namespace NConcurrency;
using namespace NCypressClient::NProto;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSequoiaClient;
using namespace NYTree;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TSequoiaServiceContextWrapper::TSequoiaServiceContextWrapper(
    ISequoiaServiceContextPtr underlyingContext)
    : TServiceContextWrapper(underlyingContext)
    , UnderlyingContext_(std::move(underlyingContext))
{ }

void TSequoiaServiceContextWrapper::SetRequestHeader(std::unique_ptr<NRpc::NProto::TRequestHeader> header)
{
    UnderlyingContext_->SetRequestHeader(std::move(header));
}

const ISequoiaTransactionPtr& TSequoiaServiceContextWrapper::GetSequoiaTransaction() const
{
    return UnderlyingContext_->GetSequoiaTransaction();
}

const TResolveResult& TSequoiaServiceContextWrapper::GetResolveResultOrThrow() const
{
    return UnderlyingContext_->GetResolveResultOrThrow();
}

TRange<TResolveStep> TSequoiaServiceContextWrapper::GetResolveHistory() const
{
    return UnderlyingContext_->GetResolveHistory();
}

std::optional<TResolveStep> TSequoiaServiceContextWrapper::TryGetLastResolveStep() const
{
    return UnderlyingContext_->TryGetLastResolveStep();
}


const ISequoiaServiceContextPtr& TSequoiaServiceContextWrapper::GetUnderlyingContext() const
{
    return UnderlyingContext_;
}

////////////////////////////////////////////////////////////////////////////////

class TSequoiaService
    : public ISequoiaService
{
public:
    explicit TSequoiaService(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void Invoke(const ISequoiaServiceContextPtr& context) override
    {
        const auto& resolveResult = context->GetResolveResultOrThrow();
        static_assert(std::variant_size<std::remove_reference_t<decltype(resolveResult)>>() == 2);
        bool isSequoiaRequest = std::holds_alternative<TSequoiaResolveResult>(resolveResult);

        if (context->GetMethod() == "Create") {
            THandlerInvocationOptions options;
            auto typedContext = New<TTypedSequoiaServiceContext<TReqCreate, TRspCreate>>(context, options);
            if (!typedContext->DeserializeRequest()) {
                return;
            }

            auto& request = typedContext->Request();
            auto type = FromProto<EObjectType>(request.type());

            switch (type) {
                case EObjectType::Rootstock: {
                    if (isSequoiaRequest) {
                        break;
                    }

                    // NB: For rootstock resolve cannot be performed on Сypress proxy, but the transaction has to be started there.
                    // We assume path is correct, if this is not the case - prepare in 2PC will fail and the error will be propagated
                    // to the user.
                    auto proxy = CreateRootstockProxy(
                        Bootstrap_,
                        context->GetSequoiaTransaction(),
                        TAbsoluteYPath(GetRequestTargetYPath(context->RequestHeader())));
                    proxy->Invoke(context);
                    return;
                }

                case EObjectType::Link: {
                    try {
                        ValidateLinkNodeCreation(context, request);
                    } catch (const std::exception& ex) {
                        context->Reply(ex);
                        return;
                    }
                    break;
                }

                default:
                    break;
            }
        }

        if (isSequoiaRequest) {
            const auto& sequoiaResolveResult = GetOrCrash<TSequoiaResolveResult>(resolveResult);
            auto prefixNodeId = sequoiaResolveResult.ResolvedPrefixNodeId;

            auto proxy = CreateNodeProxy(
                Bootstrap_,
                context->GetSequoiaTransaction(),
                prefixNodeId,
                sequoiaResolveResult.ResolvedPrefix);
            proxy->Invoke(context);
        } else {
            context->Reply(TError(
                NObjectClient::EErrorCode::RequestInvolvesCypress,
                "Cypress request has been passed to Sequoia"));
        }
    }

private:
    IBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaServicePtr CreateSequoiaService(IBootstrap* bootstrap)
{
    return New<TSequoiaService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
