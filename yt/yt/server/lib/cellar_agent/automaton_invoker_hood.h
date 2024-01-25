#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/actions/public.h>

#include <library/cpp/yt/misc/enum.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

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

    TEnumIndexedArray<EQueue, TAtomicIntrusivePtr<IInvoker>> EpochAutomatonInvokers_;
    TEnumIndexedArray<EQueue, TAtomicIntrusivePtr<IInvoker>> GuardedAutomatonInvokers_;

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
