#include "stdafx.h"
#include "cypress_service.h"
#include "node_proxy.h"

#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/rpc/message.h>

namespace NYT {
namespace NCypress {

using namespace NRpc;
using namespace NBus;
using namespace NYTree;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = CypressLogger;

////////////////////////////////////////////////////////////////////////////////

TCypressService::TCypressService(
    IInvoker* invoker,
    TCypressManager* cypressManager)
    : NRpc::TServiceBase(
        invoker,
        TCypressServiceProxy::GetServiceName(),
        CypressLogger.GetCategory())
    , CypressManager(cypressManager)
{
    YASSERT(cypressManager);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TCypressService, Execute)
{
    UNUSED(response);

    auto requestMessage = UnwrapYPathRequest(~context->GetUntypedContext());
    auto requestHeader = GetRequestHeader(~requestMessage);

    TYPath path = requestHeader.path();
    Stroka verb = requestHeader.verb();

    context->SetRequestInfo("Path: %s, Verb: %s",
        ~path,
        ~verb);

    auto processor = CypressManager->CreateRootProcessor();

    ExecuteVerb(
        ~requestMessage,
        ~processor)
    ->Subscribe(FromFunctor([=] (IMessage::TPtr responseMessage)
        {
            auto responseHeader = GetResponseHeader(~responseMessage);
            auto error = GetResponseError(responseHeader);

            context->SetResponseInfo("YPathError: %s", ~error.ToString());

            WrapYPathResponse(~context->GetUntypedContext(), ~responseMessage);
            context->Reply();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
