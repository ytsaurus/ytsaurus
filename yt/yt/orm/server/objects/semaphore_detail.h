#pragma once

#include "object.h"

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class TGeneratedSemaphore, class TProtoSemaphore>
class TSemaphoreMixin
    : public TGeneratedSemaphore
{
    using TBase = TGeneratedSemaphore;

    using TProtoSemaphoreSpec = std::decay_t<decltype(TProtoSemaphore().spec())>;
    using TProtoSemaphoreStatus = std::decay_t<decltype(TProtoSemaphore().status())>;
    using TProtoSemaphoreAcquire = std::decay_t<decltype(TProtoSemaphore().control().acquire())>;
    using TProtoSemaphorePing = std::decay_t<decltype(TProtoSemaphore().control().ping())>;
    using TProtoSemaphoreRelease = std::decay_t<decltype(TProtoSemaphore().control().release())>;
    using TProtoSemaphoreLease = typename std::decay_t<decltype(TProtoSemaphore().status().leases())>::mapped_type;

public:
    using TBase::TBase;

    //! If there is no lease with such UUID, try to acquire the new one.
    //! If there is one, then update its timeout = max(timeout, now + duration).
    void Acquire(
        TInstant now,
        const TProtoSemaphoreAcquire& control,
        const TTransactionCallContext& transactionCallContext);

    // Non-throwing equivalent of Acquire.
    NYT::TError TryAcquire(
        TInstant now,
        const TProtoSemaphoreAcquire& control,
        const TTransactionCallContext& transactionCallContext);

    //! If there is no lease with such UUID, fail.
    //! If there is one, then update its timeout = max(timeout, now + duration).
    void Ping(
        TInstant now,
        const TProtoSemaphorePing& control,
        const TTransactionCallContext& transactionCallContext);

    void Release(
        TInstant now,
        const TProtoSemaphoreRelease& control);

    THashMap<TString, const TProtoSemaphoreLease> GetFreshLeases(TInstant now) const;

    const TProtoSemaphoreLease* TryGetFreshLease(TInstant now, const TString& leaseUuid) const;

    void RemoveExpiredLeases(TInstant now);

    bool ContainsFreshLease(TInstant now, const TString& leaseUuid) const;
    bool ContainsFreshLease(TInstant now) const;

    ui64 LoadBudget() const;

    ui64 CalculateFreeBudget(TInstant now) const;

    bool IsFull(TInstant now) const;
};

////////////////////////////////////////////////////////////////////////////////

// Code shared between object and embedded semaphores.
namespace NSemaphores {

template <class TControl>
void ValidateLeaseControl(const TControl& control);

template <class TSemaphoreSpec, class TSemaphoreStatus, class TSemaphoreAcquire>
NYT::TError TryAcquire(
    TInstant now,
    const TSemaphoreSpec& spec,
    TSemaphoreStatus& status,
    const TSemaphoreAcquire& control,
    const TTransactionCallContext& transactionCallContext,
    const TString& semaphoreName);

template <class TSemaphoreStatus, class TSemaphorePing>
void Ping(
    TInstant now,
    TSemaphoreStatus& status,
    const TSemaphorePing& control,
    const TTransactionCallContext& transactionCallContext,
    const TString& semaphoreName);

template <class TSemaphoreStatus, class TSemaphoreRelease>
void Release(
    TInstant now,
    TSemaphoreStatus& status,
    const TSemaphoreRelease& control,
    const TString& semaphoreName);

template <class TEmbeddedSemaphore, class TSemaphoreAcquire>
void EmbeddedAcquire(
    TTransaction* transaction,
    TObject* object,
    const TSemaphoreAcquire& control,
    const TTransactionCallContext& transactionCallContext);

template <class TEmbeddedSemaphore, class TSemaphorePing>
void EmbeddedPing(
    TTransaction* transaction,
    TObject* object,
    const TSemaphorePing& control,
    const TTransactionCallContext& transactionCallContext);

template <class TEmbeddedSemaphore, class TSemaphoreRelease>
void EmbeddedRelease(
    TTransaction* transaction,
    TObject* object,
    const TSemaphoreRelease& control,
    const TTransactionCallContext& transactionCallContext);

} // namespace NSemaphores

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define SEMAPHORE_DETAIL_INL_H_
#include "semaphore_detail-inl.h"
#undef SEMAPHORE_DETAIL_INL_H_
