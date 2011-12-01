#include "stdafx.h"
#include "orchid_service.h"

#include "../ytree/ypath_detail.h"
#include "../ytree/ypath_client.h"

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
    NRpc::IServer* server,
    IInvoker* invoker)
    : NRpc::TServiceBase(
        invoker,
        TOrchidServiceProxy::GetServiceName(),
        OrchidLogger.GetCategory())
    , Root(root)
{
    YASSERT(root != NULL);
    YASSERT(server != NULL);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute));

    server->RegisterService(this);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD_IMPL(TOrchidService, Execute)
{
    UNUSED(request);
    UNUSED(response);

    auto requestMessage = UnwrapYPathRequest(~context->GetUntypedContext());
    auto requestParts = requestMessage->GetParts();
    YASSERT(!requestParts.empty());

    TYPath path;
    Stroka verb;
    ParseYPathRequestHeader(
        requestParts[0],
        &path,
        &verb);

    context->SetRequestInfo("Path: %s, Verb: %s",
        ~path,
        ~verb);

    auto rootService = IYPathService::FromNode(~Root);
    ExecuteVerb(
        ~rootService,
        ~requestMessage)
    ->Subscribe(FromFunctor([=] (IMessage::TPtr responseMessage)
        {
            auto responseParts = responseMessage->GetParts();
            YASSERT(!responseParts.empty());

            TError error;
            ParseYPathResponseHeader(responseParts[0], &error);

            context->SetRequestInfo("YPathError: %s", ~error.ToString());

            WrapYPathResponse(~context->GetUntypedContext(), ~responseMessage);
            context->Reply();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT

