#include "stdafx.h"
#include "persistent_response_keeper.h"
#include "composite_automaton.h"

#include <core/concurrency/periodic_executor.h>

#include <core/rpc/response_keeper_detail.h>

#include <server/hydra/persistent_response_keeper.pb.h>

namespace NYT {
namespace NHydra {

using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto EvictionPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

class TPersistentResponseKeeper::TImpl
    : public TResponseKeeperBase
    , public TCompositeAutomatonPart
    , public IResponseKeeper
{
public:
    TImpl(
        TResponseKeeperConfigPtr config,
        IInvokerPtr automatonInvoker,
        IHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr compositeAutomaton,
        const NProfiling::TProfiler& profiler)
        : TResponseKeeperBase(config, profiler)
        , TCompositeAutomatonPart(hydraManager, compositeAutomaton)
        , AutomatonInvoker_(automatonInvoker)
    {
        RegisterMethod(BIND(&TImpl::HydraEvictExpiredResponses, Unretained(this)));

        RegisterLoader(
            "ResponseKeeper",
            BIND(&TImpl::Load, Unretained(this)));
        RegisterSaver(
            ESerializationPriority::Values,
            "ResponseKeeper",
            BIND(&TImpl::Save, Unretained(this)));
    }

    virtual TFuture<TSharedRefArray> TryBeginRequest(const TMutationId& id) override
    {
        return TResponseKeeperBase::TryBeginRequest(id);
    }

    virtual void EndRequest(const TMutationId& id, TSharedRefArray response) override
    {
        const auto* mutationContext = HydraManager->GetMutationContext();
        TResponseKeeperBase::EndRequest(
            id,
            std::move(response),
            mutationContext ? mutationContext->GetTimestamp() : TInstant(),
            mutationContext != nullptr);
    }

private:
    IInvokerPtr AutomatonInvoker_;

    TPeriodicExecutorPtr EvictionExecutor_;


    virtual void OnLeaderActive() override
    {
        YCHECK(!EvictionExecutor_);
        EvictionExecutor_ = New<TPeriodicExecutor>(
            AutomatonInvoker_,
            BIND(&TImpl::OnEvict, MakeWeak(this)),
            EvictionPeriod);
        EvictionExecutor_->Start();
    }

    virtual void OnStopLeading() override
    {
        YCHECK(EvictionExecutor_);
        EvictionExecutor_->Stop();
        EvictionExecutor_.Reset();
    }


    void OnEvict()
    {
        CreateMutation(HydraManager, NProto::TReqEvictExpiredResponses())
            ->Commit();
    }

    void HydraEvictExpiredResponses(const NProto::TReqEvictExpiredResponses& /*request*/)
    {
        const auto* mutationContext = HydraManager->GetMutationContext();
        EvictExpiredResponses(mutationContext->GetTimestamp());
    }


    void Load(TLoadContext& context)
    {
        TResponseKeeperBase::Load(context);
    }

    void Save(TSaveContext& context)
    {
        TResponseKeeperBase::Save(context);
    }

    virtual void Clear() override
    {
        TResponseKeeperBase::Clear();
    }

};

////////////////////////////////////////////////////////////////////////////////

TPersistentResponseKeeper::TPersistentResponseKeeper(
    TResponseKeeperConfigPtr config,
    IInvokerPtr automatonInvoker,
    IHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr compositeAutomaton,
    const NProfiling::TProfiler& profiler)
    : Impl_(New<TImpl>(
        config,
        automatonInvoker,
        hydraManager,
        compositeAutomaton,
        profiler))
{ }

TPersistentResponseKeeper::~TPersistentResponseKeeper()
{ }

IResponseKeeperPtr TPersistentResponseKeeper::GetResponseKeeper()
{
    return Impl_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
