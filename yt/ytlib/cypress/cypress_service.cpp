#include "cypress_service.h"
//#include "registry_service.pb.h"
//
//#include "../misc/foreach.h"
//#include "../misc/serialize.h"
//#include "../misc/guid.h"
//#include "../misc/assert.h"
//#include "../misc/string.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = CypressLogger;

////////////////////////////////////////////////////////////////////////////////

TCypressService::TCypressService(
    const TConfig& config,
    IInvoker::TPtr serviceInvoker,
    NRpc::TServer::TPtr server,
    TCypressState::TPtr state)
    : TMetaStateServiceBase(
        serviceInvoker,
        TCypressServiceProxy::GetServiceName(),
        CypressLogger.GetCategory())
    , Config(config)
    , State(state)
{
    RegisterMethods();
    server->RegisterService(this);
}

void TCypressService::RegisterMethods()
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Get));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Set));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Lock));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Remove));
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TCypressService, Get)
{
    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();

    context->SetRequestInfo("TransactionId: %s, Path: %s",
        ~transactionId.ToString(),
        ~path);

    UNUSED(response);
    YASSERT(false);
}

RPC_SERVICE_METHOD_IMPL(TCypressService, Set)
{
    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();
    Stroka value = request->GetValues();

    context->SetRequestInfo("TransactionId: %s, Path: %s",
        ~transactionId.ToString(),
        ~path);

    UNUSED(response);
    YASSERT(false);
}

RPC_SERVICE_METHOD_IMPL(TCypressService, Remove)
{
    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();

    context->SetRequestInfo("TransactionId: %s, Path: %s",
        ~transactionId.ToString(),
        ~path);

    UNUSED(response);
    YASSERT(false);
}

RPC_SERVICE_METHOD_IMPL(TCypressService, Lock)
{
    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();
    auto mode = ELockMode(request->GetMode());

    context->SetRequestInfo("TransactionId: %s, Path: %s, Mode: %s",
        ~transactionId.ToString(),
        ~path,
        ~mode.ToString());

    UNUSED(response);
    YASSERT(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
