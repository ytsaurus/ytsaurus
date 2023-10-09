#ifndef AUTOMATON_INVOKER_HOOD_INL_H_
#error "Direct inclusion of this file is not allowed, include automaton_invoker_hood.h"
// For the sake of sane code completion.
#include "automaton_invoker_hood.h"
#endif

#include <yt/yt/server/lib/hydra/hydra_manager.h>

#include <yt/yt/core/concurrency/fair_share_action_queue.h>

#include <yt/yt/core/actions/invoker_util.h>
#include <yt/yt/core/actions/cancelable_context.h>

#include <util/system/guard.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

template <typename EQueue>
TAutomatonInvokerHood<EQueue>::TAutomatonInvokerHood(const TString& threadName)
    : AutomatonQueue_(NConcurrency::CreateEnumIndexedFairShareActionQueue<EQueue>(threadName))
{
    ResetEpochInvokers();
    ResetGuardedInvokers();
}

template <typename EQueue>
IInvokerPtr TAutomatonInvokerHood<EQueue>::GetAutomatonInvoker(EQueue queue) const
{
    return AutomatonQueue_->GetInvoker(queue);
}

template <typename EQueue>
IInvokerPtr TAutomatonInvokerHood<EQueue>::GetEpochAutomatonInvoker(EQueue queue) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return EpochAutomatonInvokers_[queue].Acquire();
}

template <typename EQueue>
IInvokerPtr TAutomatonInvokerHood<EQueue>::GetGuardedAutomatonInvoker(EQueue queue) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return GuardedAutomatonInvokers_[queue].Acquire();
}

template <typename EQueue>
void TAutomatonInvokerHood<EQueue>::InitEpochInvokers(const NHydra::IHydraManagerPtr& hydraManager)
{
    if (!hydraManager) {
        return;
    }

    for (auto queue : TEnumTraits<EQueue>::GetDomainValues()) {
        EpochAutomatonInvokers_[queue].Store(
            hydraManager
                ->GetAutomatonCancelableContext()
                ->CreateInvoker(GetAutomatonInvoker(queue)));
    }
}

template <typename EQueue>
void TAutomatonInvokerHood<EQueue>::ResetEpochInvokers()
{
    VERIFY_THREAD_AFFINITY_ANY();

    for (auto& invoker : EpochAutomatonInvokers_) {
        invoker.Store(GetNullInvoker());
    }
}

template <typename EQueue>
void TAutomatonInvokerHood<EQueue>::InitGuardedInvokers(const NHydra::IHydraManagerPtr& hydraManager)
{
    if (!hydraManager) {
        return;
    }

    for (auto queue : TEnumTraits<EQueue>::GetDomainValues()) {
        auto unguardedInvoker = GetAutomatonInvoker(queue);
        GuardedAutomatonInvokers_[queue].Store(
            hydraManager->CreateGuardedAutomatonInvoker(unguardedInvoker));
    }
}

template <typename EQueue>
void TAutomatonInvokerHood<EQueue>::ResetGuardedInvokers()
{
    VERIFY_THREAD_AFFINITY_ANY();

    for (auto& invoker : GuardedAutomatonInvokers_) {
        invoker.Store(GetNullInvoker());
    }
}

////// /////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
