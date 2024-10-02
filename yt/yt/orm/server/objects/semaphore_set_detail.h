#pragma once

#include "semaphore_detail.h"

#include <yt/yt/core/misc/mpl.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <
    class TGeneratedSemaphoreSet,
    NMpl::DerivedFromSpecializationOf<TSemaphoreMixin> TSemaphore,
    class TProtoSemaphoreSet,
    class TProtoSemaphore>
class TSemaphoreSetMixin
    : public TGeneratedSemaphoreSet
{
    using TBase = TGeneratedSemaphoreSet;

    using TProtoSemaphoreSetSpec = std::decay_t<decltype(TProtoSemaphoreSet().spec())>;
    using TProtoSemaphoreSetStatus = std::decay_t<decltype(TProtoSemaphoreSet().status())>;
    using TProtoSemaphoreSetAcquire = std::decay_t<decltype(TProtoSemaphoreSet().control().acquire())>;
    using TProtoSemaphoreSetPing = std::decay_t<decltype(TProtoSemaphoreSet().control().ping())>;
    using TProtoSemaphoreSetRelease = std::decay_t<decltype(TProtoSemaphoreSet().control().release())>;
    using TProtoSemaphoreAcquire = std::decay_t<decltype(TProtoSemaphore().control().acquire())>;
    using TProtoSemaphorePing = std::decay_t<decltype(TProtoSemaphore().control().ping())>;
    using TProtoSemaphoreRelease = std::decay_t<decltype(TProtoSemaphore().control().release())>;
    using TProtoSemaphoreLease = typename std::decay_t<decltype(TProtoSemaphore().status().leases())>::mapped_type;

public:
    using TBase::TBase;

    void Acquire(
        TInstant now,
        const TProtoSemaphoreSetAcquire& control,
        const TTransactionCallContext& transactionCallContext);

    NYT::TError TryAcquire(
        TInstant now,
        const TProtoSemaphoreSetAcquire& control,
        const TTransactionCallContext& transactionCallContext);

    void Ping(
        TInstant now,
        const TProtoSemaphoreSetPing& control,
        const TTransactionCallContext& transactionCallContext);

    void Release(
        TInstant now,
        const TProtoSemaphoreSetRelease& control);

    THashMap<TString, THashMap<TString, const TProtoSemaphoreLease>> GetFreshLeasesBySemaphore(TInstant now) const;

    template <class TGeneratedSemaphore>
    static void ScheduleSemaphoresLoad(const std::vector<TGeneratedSemaphore*>& semaphores);

    template <class TSemaphoreControl>
    auto CalculateLeaseDuration(const TSemaphoreControl& control) const
    {
        if (!control.duration()) {
            return TBase::Spec().Etc().Load().default_duration();
        }
        return control.duration();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define SEMAPHORE_SET_DETAIL_INL_H_
#include "semaphore_set_detail-inl.h"
#undef SEMAPHORE_SET_DETAIL_INL_H_
