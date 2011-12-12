#include "stdafx.h"
#include "cypress_service.h"
#include "node_proxy.h"

#include "../ytree/ypath_detail.h"
#include "../ytree/ypath_client.h"

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
    TCypressManager* cypressManager,
    TTransactionManager* transactionManager,
    NRpc::IRpcServer* server)
    : NRpc::TServiceBase(
        invoker,
        TCypressServiceProxy::GetServiceName(),
        CypressLogger.GetCategory())
    , CypressManager(cypressManager)
    , TransactionManager(transactionManager)
{
    YASSERT(cypressManager != NULL);
    YASSERT(server != NULL);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute));

    server->RegisterService(this);
}

void TCypressService::ValidateTransactionId(const TTransactionId& transactionId)
{
    if (transactionId != NullTransactionId &&
        TransactionManager->FindTransaction(transactionId) == NULL)
    {
        ythrow TServiceException(EErrorCode::NoSuchTransaction) << 
            Sprintf("No such transaction (TransactionId: %s)", ~transactionId.ToString());
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TCypressService, Execute)
{
    UNUSED(response);

    auto transactionId = TTransactionId::FromProto(request->transactionid());

    auto requestMessage = UnwrapYPathRequest(~context->GetUntypedContext());
    auto requestParts = requestMessage->GetParts();
    YASSERT(!requestParts.empty());

    TYPath path;
    Stroka verb;
    ParseYPathRequestHeader(
        requestParts[0],
        &path,
        &verb);

    context->SetRequestInfo("TransactionId: %s, Path: %s, Verb: %s",
        ~transactionId.ToString(),
        ~path,
        ~verb);

    ValidateTransactionId(transactionId);

    auto rootNode = CypressManager->GetNodeProxy(RootNodeId, transactionId);
    auto rootService = IYPathService::FromNode(~rootNode);

    ExecuteVerb(
        ~rootService,
        ~requestMessage,
        ~CypressManager)
    ->Subscribe(FromFunctor([=] (IMessage::TPtr responseMessage)
        {
            auto responseParts = responseMessage->GetParts();
            YASSERT(!responseParts.empty());

            TError error;
            ParseYPathResponseHeader(responseParts[0], &error);

            context->SetResponseInfo("YPathError: %s", ~error.ToString());

            WrapYPathResponse(~context->GetUntypedContext(), ~responseMessage);
            context->Reply();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
