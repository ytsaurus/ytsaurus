#pragma once

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/enum.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

template <typename EQueue>
class TAutomatonInvokerHood
{
public:
    explicit TAutomatonInvokerHood(const TString& threadName);

    IInvokerPtr GetAutomatonInvoker(EQueue queue = EQueue::Default) const;
    IInvokerPtr GetEpochAutomatonInvoker(EQueue queue = EQueue::Default) const;
    IInvokerPtr GetGuardedAutomatonInvoker(EQueue queue = EQueue::Default) const;

protected:
    const NConcurrency::IEnumIndexedFairShareActionQueuePtr<EQueue> AutomatonQueue_;

    TEnumIndexedVector<EQueue, TAtomicObject<IInvokerPtr>> EpochAutomatonInvokers_;
    TEnumIndexedVector<EQueue, TAtomicObject<IInvokerPtr>> GuardedAutomatonInvokers_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void InitEpochInvokers(const NHydra::IHydraManagerPtr& hydraManager);
    void InitGuardedInvokers(const NHydra::IHydraManagerPtr& hydraManager);
    void ResetEpochInvokers();
    void ResetGuardedInvokers();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent

#define AUTOMATON_INVOKER_HOOD_INL_H_
#include "automaton_invoker_hood-inl.h"
#undef AUTOMATON_INVOKER_HOOD_INL_H_
