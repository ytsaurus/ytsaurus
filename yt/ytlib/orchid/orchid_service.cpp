#include "stdafx.h"
#include "orchid_service.h"

#include <core/ytree/ypath_detail.h>
#include <core/ytree/ypath_client.h>

#include <core/rpc/message.h>

namespace NYT {
namespace NOrchid {

using namespace NBus;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = OrchidLogger;

////////////////////////////////////////////////////////////////////////////////

TOrchidService::TOrchidService(
    NYTree::INodePtr root,
    IInvokerPtr invoker)
    : NRpc::TServiceBase(
        invoker,
        TOrchidServiceProxy::GetServiceName(),
        OrchidLogger.GetCategory())
{
    YCHECK(root);

    RootService = CreateRootService(root);
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TOrchidService, Execute)
{
    UNUSED(request);
    UNUSED(response);

    auto requestMessage = TSharedRefArray(request->Attachments());

    NRpc::NProto::TRequestHeader requestHeader;
    if (!ParseRequestHeader(requestMessage, &requestHeader)) {
        THROW_ERROR_EXCEPTION("Error parsing request header");
    }

    const auto& path = requestHeader.service();
    const auto& verb = requestHeader.verb();

    context->SetRequestInfo("Path: %s, Verb: %s",
        ~path,
        ~verb);

    ExecuteVerb(RootService, requestMessage)
        .Subscribe(BIND([=] (TSharedRefArray responseMessage) {
            NRpc::NProto::TResponseHeader responseHeader;
            YCHECK(ParseResponseHeader(responseMessage, &responseHeader));

            auto error = FromProto(responseHeader.error());

            context->SetResponse("InnerError: %s", ~ToString(error));

            response->Attachments() = responseMessage.ToVector();
            context->Reply();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT

