#include "cypress_service.h"

#include "../ytree/ypath.h"
#include "../ytree/yson_reader.h"
#include "../ytree/yson_writer.h"

//#include "registry_service.pb.h"
//
//#include "../misc/foreach.h"
//#include "../misc/serialize.h"
//#include "../misc/guid.h"
//#include "../misc/assert.h"
//#include "../misc/string.h"

namespace NYT {
namespace NCypress {

using namespace NYTree;

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

    // TODO: validate transaction id

    auto root = State->GetNode(transactionId, RootNodeId);

    Stroka output;
    TStringOutput outputStream(output);
    TYsonWriter writer(&outputStream, false); // TODO: use binary

    try {
        GetYPath(AsYPath(root), path, &writer);
    } catch (...) {
        // TODO:
        context->Reply(EErrorCode::ShitHappens);
    }

    response->SetValue(output);
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TCypressService, Set)
{
    UNUSED(response);

    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();
    Stroka value = request->GetValue();

    context->SetRequestInfo("TransactionId: %s, Path: %s",
        ~transactionId.ToString(),
        ~path);

    // TODO: validate transaction id

    auto root = State->GetNode(transactionId, RootNodeId);

    TStringInput inputStream(request->GetValue());
    auto producer = TYsonReader::GetProducer(&inputStream);

    try {
        SetYPath(AsYPath(root), path, producer);
    } catch (...) {
        // TODO:
        context->Reply(EErrorCode::ShitHappens);
    }

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TCypressService, Remove)
{
    UNUSED(response);

    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();

    context->SetRequestInfo("TransactionId: %s, Path: %s",
        ~transactionId.ToString(),
        ~path);

    auto root = State->GetNode(transactionId, RootNodeId);

    // TODO: validate transaction id

    try {
        RemoveYPath(AsYPath(root), path);
    } catch (...) {
        // TODO:
        context->Reply(EErrorCode::ShitHappens);
    }

    context->Reply();
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
