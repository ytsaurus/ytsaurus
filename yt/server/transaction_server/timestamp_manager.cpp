#include "stdafx.h"
#include "timestamp_manager.h"
#include "config.h"
#include "private.h"

#include <core/actions/invoker_util.h>

#include <core/concurrency/thread_affinity.h>

#include <core/rpc/service_detail.h>
#include <core/rpc/server.h>

#include <ytlib/transaction_client/timestamp_service_proxy.h>

#include <server/hydra/composite_automaton.h>
#include <server/hydra/hydra_manager.h>

namespace NYT {
namespace NTransactionServer {

using namespace NRpc;
using namespace NHydra;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TransactionServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTimestampManager::TImpl
    : public TServiceBase
    , public TCompositeAutomatonPart
{
public:
    TImpl(
        TTimestampManagerConfigPtr config,
        IInvokerPtr automatonInvoker,
        IRpcServerPtr rpcServer,
        IHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton)
        : TServiceBase(
            GetSyncInvoker(),
            TTimestampServiceProxy::GetServiceName(),
            TransactionServerLogger.GetCategory())
        , TCompositeAutomatonPart(
            hydraManager,
            automaton)
        , Config(config)
        , AutomatonInvoker(automatonInvoker)
        , CurrentTimestamp(0)
    {
        automaton->RegisterPart(this);
        rpcServer->RegisterService(this);

        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTimestamp));

        RegisterLoader(
            "TimestampManager",
            BIND(&TImpl::Load, Unretained(this)));
        RegisterSaver(
            ESerializationPriority::Values,
            "TimestampManager",
            BIND(&TImpl::Save, Unretained(this)));
    }

private:
    typedef TImpl TThis;

    TTimestampManagerConfigPtr Config;
    IInvokerPtr AutomatonInvoker;

    TAtomic CurrentTimestamp;


    // RPC handlers.

    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, GetTimestamp)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        response->set_timestamp(AtomicIncrement(CurrentTimestamp));
        context->Reply();
    }


    virtual void Clear() override
    {
        CurrentTimestamp = 0;
    }

    void Load(TLoadContext& context)
    {
        // TODO(babenko)
    }

    void Save(TSaveContext& context) const
    {
        // TODO(babenko)
    }

};

////////////////////////////////////////////////////////////////////////////////

TTimestampManager::TTimestampManager(
    TTimestampManagerConfigPtr config,
    IInvokerPtr automatonInvoker,
    NRpc::IRpcServerPtr rpcServer,
    NHydra::IHydraManagerPtr hydraManager,
    NHydra::TCompositeAutomatonPtr automaton)
    : Impl(New<TImpl>(
        config,
        automatonInvoker,
        rpcServer,
        hydraManager,
        automaton))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
