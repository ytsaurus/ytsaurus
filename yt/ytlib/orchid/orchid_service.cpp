#include "stdafx.h"
#include "orchid_service.h"

#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/rpc/message.h>

namespace NYT {
namespace NOrchid {

using namespace NBus;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger(OrchidLogger);

////////////////////////////////////////////////////////////////////////////////

TOrchidService::TOrchidService(
    NYTree::INodePtr root,
    IInvoker::TPtr invoker)
    : NRpc::TServiceBase(
        invoker,
        TOrchidServiceProxy::GetServiceName(),
        OrchidLogger.GetCategory())
{
    YASSERT(root);

    RootService = CreateRootService(root);
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TOrchidService, Execute)
{
    UNUSED(request);
    UNUSED(response);

    auto requestMessage = CreateMessageFromParts(request->Attachments());
    auto requestHeader = GetRequestHeader(~requestMessage);

    TYPath path = requestHeader.path();
    Stroka verb = requestHeader.verb();

    context->SetRequestInfo("Path: %s, Verb: %s",
        ~path,
        ~verb);

    ExecuteVerb(~RootService, ~requestMessage)
        .Subscribe(BIND([=] (IMessage::TPtr responseMessage) {
            auto responseHeader = GetResponseHeader(~responseMessage);
            auto error = TError::FromProto(responseHeader.error());

            context->SetRequestInfo("Error: %s", ~error.ToString());

            response->Attachments() = responseMessage->GetParts();
            context->Reply();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT

