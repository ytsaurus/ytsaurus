#include "stdafx.h"
#include "cypress_service.h"
#include "node_proxy.h"

#include "../ytree/ypath_detail.h"

namespace NYT {
namespace NCypress {

using namespace NBus;
using namespace NYTree;
using namespace NMetaState;
using namespace NTransaction;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = CypressLogger;

////////////////////////////////////////////////////////////////////////////////

TCypressService::TCypressService(
    IInvoker* invoker,
    TCypressManager* cypressManager,
    TTransactionManager* transactionManager,
    NRpc::IServer* server)
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
            Sprintf("Invalid transaction id (TransactionId: %s)", ~transactionId.ToString());
    }
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TCypressService, Execute)
{
    UNUSED(response);

    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    auto rootNodeId =
        request->HasRootNodeId()
        ? TNodeId::FromProto(request->GetRootNodeId())
        : RootNodeId;

    const auto& attachments = request->Attachments();

    TYPath path;
    Stroka verb;
    YASSERT(!attachments.empty());
    ParseYPathRequestHeader(
        attachments[0],
        &path,
        &verb);

    context->SetRequestInfo("TransactionId: %s, Path: %s, Verb: %s, RootNodeId: %s",
        ~transactionId.ToString(),
        ~path,
        ~verb,
        ~rootNodeId.ToString());

    ValidateTransactionId(transactionId);

    auto root = CypressManager->FindNodeProxy(rootNodeId, transactionId);
    if (~root == NULL) {
        ythrow TServiceException(EErrorCode::ResolutionError) << Sprintf("Root node is not found (NodeId: %s)",
            ~rootNodeId.ToString());
    }

    auto rootService = IYPathService::FromNode(~root);

    IYPathService::TPtr suffixService;
    TYPath suffixPath;
    try {
        ResolveYPath(~rootService, path, verb, &suffixService, &suffixPath);
    } catch (...) {
        ythrow TServiceException(EErrorCode::ResolutionError) << CurrentExceptionMessage();
    }

    LOG_DEBUG("Execute: SuffixPath: %s", ~suffixPath);

    auto requestMessage = UnwrapYPathRequest(~context->GetUntypedContext());
    auto updatedRequestMessage = UpdateYPathRequestHeader(~requestMessage, suffixPath, verb);
    auto innerContext = CreateYPathContext(
        ~updatedRequestMessage,
        suffixPath,
        verb,
        Logger.GetCategory(),
        ~FromFunctor([=] (const TYPathResponseHandlerParam& param)
            {
                WrapYPathResponse(~context->GetUntypedContext(), ~param.Message);
                context->Reply();
            }));

    CypressManager->ExecuteVerb(~suffixService, ~innerContext);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
