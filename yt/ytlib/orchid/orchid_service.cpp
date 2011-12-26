#include "stdafx.h"
#include "orchid_service.h"

#include "../ytree/ypath_detail.h"
#include "../ytree/ypath_client.h"

#include "../rpc/message.h"

namespace NYT {
namespace NOrchid {

using namespace NBus;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger(OrchidLogger);

////////////////////////////////////////////////////////////////////////////////

TOrchidService::TOrchidService(
    NYTree::INode* root,
    IInvoker* invoker)
    : NRpc::TServiceBase(
        invoker,
        TOrchidServiceProxy::GetServiceName(),
        OrchidLogger.GetCategory())
    , Root(root)
{
    YASSERT(root);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TOrchidService, Execute)
{
    UNUSED(request);
    UNUSED(response);

    auto requestMessage = UnwrapYPathRequest(~context->GetUntypedContext());
    auto requestHeader = GetRequestHeader(~requestMessage);
    TYPath path = requestHeader.path();
    Stroka verb = requestHeader.verb();

    context->SetRequestInfo("Path: %s, Verb: %s",
        ~path,
        ~verb);

    auto rootService = IYPathService::FromNode(~Root);
    ExecuteVerb(
        ~rootService,
        ~requestMessage)
    ->Subscribe(FromFunctor([=] (IMessage::TPtr responseMessage)
        {
            auto responseHeader = GetResponseHeader(~responseMessage);
            auto error = GetResponseError(responseHeader);

            context->SetRequestInfo("YPathError: %s", ~error.ToString());

            WrapYPathResponse(~context->GetUntypedContext(), ~responseMessage);
            context->Reply();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT

