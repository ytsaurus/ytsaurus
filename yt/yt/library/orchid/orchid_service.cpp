#include "orchid_service_proxy.h"
#include "orchid_service.h"
#include "private.h"

#include <yt/yt/core/rpc/message.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_detail.h>

namespace NYT::NOrchid {

using namespace NBus;
using namespace NRpc;
using namespace NRpc::NProto;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TOrchidService
    : public TServiceBase
{
public:
    TOrchidService(
        INodePtr root,
        IInvokerPtr invoker,
        IAuthenticatorPtr authenticator)
        : TServiceBase(
            invoker,
            TOrchidServiceProxy::GetDescriptor(),
            OrchidLogger(),
            TServiceOptions{
                .Authenticator = std::move(authenticator),
            })
        , RootService_(CreateRootService(root))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetCancelable(true));
    }

private:
    const IYPathServicePtr RootService_;

    DECLARE_RPC_SERVICE_METHOD(NProto, Execute)
    {
        auto requestMessage = TSharedRefArray(request->Attachments(), TSharedRefArray::TCopyParts{});

        TRequestHeader requestHeader;
        if (!TryParseRequestHeader(requestMessage, &requestHeader)) {
            THROW_ERROR_EXCEPTION("Error parsing request header");
        }

        context->SetRequestInfo("%v.%v %v",
            requestHeader.service(),
            requestHeader.method(),
            GetRequestTargetYPath(requestHeader));

        ExecuteVerb(RootService_, requestMessage)
            .Subscribe(BIND([=] (const TErrorOr<TSharedRefArray>& responseMessageOrError) {
                if (!responseMessageOrError.IsOK()) {
                    context->Reply(responseMessageOrError);
                    return;
                }

                const auto& responseMessage = responseMessageOrError.Value();

                TResponseHeader responseHeader;
                if (!TryParseResponseHeader(responseMessage, &responseHeader)) {
                    context->Reply(TError(NRpc::EErrorCode::ProtocolError, "Error parsing response header"));
                    return;
                }

                auto error = FromProto<TError>(responseHeader.error());
                context->SetResponseInfo("InnerError: %v", error);

                response->Attachments() = responseMessage.ToVector();

                context->Reply();
            }));
    }

};

IServicePtr CreateOrchidService(
    INodePtr root,
    IInvokerPtr invoker,
    IAuthenticatorPtr authenticator)
{
    return New<TOrchidService>(
        std::move(root),
        std::move(invoker),
        std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrchid
