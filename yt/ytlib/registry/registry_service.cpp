#include "registry_service.h"
#include "registry_service.pb.h"

#include "../misc/foreach.h"
#include "../misc/serialize.h"
#include "../misc/guid.h"
#include "../misc/assert.h"
#include "../misc/string.h"

namespace NYT {
namespace NRegistry {

using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RegistryLogger;

////////////////////////////////////////////////////////////////////////////////

class TRegistryService::TState
    : public NMetaState::TMetaStatePart
    , public NTransaction::ITransactionHandler
{
public:
    typedef TIntrusivePtr<TState> TPtr;

    TState(
        const TConfig& config,
        NMetaState::TMetaStateManager::TPtr metaStateManager,
        NMetaState::TCompositeMetaState::TPtr metaState,
        TTransactionManager::TPtr transactionManager)
        : TMetaStatePart(metaStateManager, metaState)
        , Config(config)
        , TransactionManager(transactionManager)
    {
//        RegisterMethod(this, &TState::AddChunk);

        transactionManager->RegisterHander(this);
    }

private:
    TConfig Config;
    TTransactionManager::TPtr TransactionManager;

    // TMetaStatePart overrides.
    virtual Stroka GetPartName() const
    {
        return "RegistryService";
    }

    virtual TFuture<TVoid>::TPtr Save(TOutputStream* stream)
    {
    	YASSERT(false);
        UNUSED(stream);
    	return NULL;
    }

    virtual TFuture<TVoid>::TPtr Load(TInputStream* stream)
    {
    	YASSERT(false);
        UNUSED(stream);
    	return NULL;
    }

    virtual void Clear()
    {
    }

    // ITransactionHandler overrides.
    virtual void OnTransactionStarted(TTransaction& transaction)
    {
        UNUSED(transaction);
    }

    virtual void OnTransactionCommitted(TTransaction& transaction)
    {
        UNUSED(transaction);
    }

    virtual void OnTransactionAborted(TTransaction& transaction)
    {
        UNUSED(transaction);
    }
};

////////////////////////////////////////////////////////////////////////////////

TRegistryService::TRegistryService(
    const TConfig& config,
    NMetaState::TMetaStateManager::TPtr metaStateManager,
    NMetaState::TCompositeMetaState::TPtr metaState,
    NRpc::TServer::TPtr server,
    TTransactionManager::TPtr transactionManager)
    : TMetaStateServiceBase(
        metaState->GetInvoker(),
        TRegistryServiceProxy::GetServiceName(),
        RegistryLogger.GetCategory())
    , Config(config)
    , TransactionManager(transactionManager)
    , State(New<TState>(
        config,
        metaStateManager,
        metaState,
        transactionManager))
{
    RegisterMethods();
    metaState->RegisterPart(~State);
    server->RegisterService(this);
}

void TRegistryService::RegisterMethods()
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Get));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Set));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Lock));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Remove));
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TRegistryService, Get)
{
    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();

    context->SetRequestInfo("TransactionId: %s, Path: %s",
        ~transactionId.ToString(),
        ~path);

    UNUSED(response);
    YASSERT(false);
}

RPC_SERVICE_METHOD_IMPL(TRegistryService, Set)
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

RPC_SERVICE_METHOD_IMPL(TRegistryService, Remove)
{
    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();

    context->SetRequestInfo("TransactionId: %s, Path: %s",
        ~transactionId.ToString(),
        ~path);

    UNUSED(response);
    YASSERT(false);
}

RPC_SERVICE_METHOD_IMPL(TRegistryService, Lock)
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

} // namespace NRegistry
} // namespace NYT
