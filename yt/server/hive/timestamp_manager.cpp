#include "stdafx.h"
#include "timestamp_manager.h"
#include "config.h"
#include "private.h"

#include <core/actions/invoker_util.h>

#include <core/misc/serialize.h>

#include <core/concurrency/thread_affinity.h>

#include <core/rpc/service_detail.h>
#include <core/rpc/server.h>

#include <ytlib/hive/timestamp_service_proxy.h>

#include <server/hydra/composite_automaton.h>
#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation.h>

#include <server/hive/timestamp_manager.pb.h>

#include <atomic>

namespace NYT {
namespace NHive {

using namespace NRpc;
using namespace NHydra;
using namespace NHive::NProto;

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
            // TODO(babenko): use sync invoker 
            automatonInvoker,
            TTimestampServiceProxy::GetServiceName(),
            HiveLogger.GetCategory())
        , TCompositeAutomatonPart(
            hydraManager,
            automaton)
        , Config(config)
        , AutomatonInvoker(automatonInvoker)
        , Committing(false)
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

        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCommitTimestamp, Unretained(this)));
    }

private:
    typedef TImpl TThis;

    TTimestampManagerConfigPtr Config;
    IInvokerPtr AutomatonInvoker;

    //! Currently available timestamp.
    std::atomic<TTimestamp> CurrentTimestamp;
    
    //! Commit must be issued when current timestamp reaches this watermark.
    std::atomic<TTimestamp> WatermarkTimestamp;
    
    //! The last persistently committed timestamp.
    //! All returned timestamps must be strictly less than this one.
    std::atomic<TTimestamp> PersistentTimestamp;


    // RPC handlers.

    DECLARE_RPC_SERVICE_METHOD(NProto, GetTimestamp)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        GetTimestampImpl(std::move(context));
    }

    void GetTimestampImpl(TCtxGetTimestampPtr context)
    {
        auto candindateTimestamp = CurrentTimestamp++;

        if (candindateTimestamp >= PersistentTimestamp.load()) {
            --CurrentTimestamp;
            TGuard<TSpinLock> guard(SpinLock);
            StartCommit();
            PendingContexts.push_back(std::move(context));
            return;
        } else if (candindateTimestamp >= WatermarkTimestamp.load()) {
            TGuard<TSpinLock> guard(SpinLock);
            StartCommit();
        }

        auto& response = context->Response();
        response.set_timestamp(candindateTimestamp);
        context->Reply();
    }

    TSpinLock SpinLock;
    bool Committing;
    std::vector<TCtxGetTimestampPtr> PendingContexts;


    void StartCommit()
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock);

        if (Committing)
            return;

        TReqCommitTimestamp hydraRequest;
        hydraRequest.set_timestamp(PersistentTimestamp.load() + Config->BatchSize);
        CreateMutation(HydraManager, AutomatonInvoker, hydraRequest)
            ->OnSuccess(BIND(&TImpl::OnCommitSuccess, MakeStrong(this)))
            ->OnError(BIND(&TImpl::OnCommitFailure, MakeStrong(this)))
            ->Commit();

        Committing = true;
    }

    std::vector<TCtxGetTimestampPtr> FinishCommit()
    {
        std::vector<TCtxGetTimestampPtr> pendingContexts;
        {
            TGuard<TSpinLock> guard(SpinLock);
            PendingContexts.swap(pendingContexts);
            Committing = false;
        }
        return pendingContexts;
    }

    void OnCommitSuccess()
    {
        auto pendingContexts = FinishCommit();
        for (auto context : pendingContexts) {
            GetTimestampImpl(std::move(context));
        }
    }

    void OnCommitFailure(const TError& error)
    {
        auto pendingContexts = FinishCommit();
        for (auto context : pendingContexts) {
            context->Reply(error);
        }
    }


    virtual void Clear() override
    {
        CurrentTimestamp = 0;
        WatermarkTimestamp = 0;
        PersistentTimestamp = 0;
    }

    void Load(TLoadContext& context)
    {
        auto timestamp = NYT::Load<TTimestamp>(context);
        CurrentTimestamp = timestamp;
        WatermarkTimestamp = timestamp;
        PersistentTimestamp = timestamp;
    }

    void Save(TSaveContext& context) const
    {
        NYT::Save(context, PersistentTimestamp.load());
    }


    void HydraCommitTimestamp(const TReqCommitTimestamp& request)
    {
        PersistentTimestamp = request.timestamp();
        WatermarkTimestamp = PersistentTimestamp - Config->BatchSize / 2;
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

} // namespace NHive
} // namespace NYT
