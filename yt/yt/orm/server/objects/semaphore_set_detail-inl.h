#ifndef SEMAPHORE_SET_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include semaphore_set_detail.h"
// For the sake of sane code completion.
#include "semaphore_set_detail.h"
#endif

#include "semaphore_detail.h"
#include "private.h"

#include <util/random/shuffle.h>

namespace NYT::NOrm::NServer::NObjects {

#define T_SEMAPHORE_SET_MIXIN TSemaphoreSetMixin<TGeneratedSemaphoreSet, TSemaphore, TProtoSemaphoreSet, TProtoSemaphore>

////////////////////////////////////////////////////////////////////////////////

template <
    class TGeneratedSemaphoreSet,
    NMpl::DerivedFromSpecializationOf<TSemaphoreMixin> TSemaphore,
    class TProtoSemaphoreSet,
    class TProtoSemaphore>
TError T_SEMAPHORE_SET_MIXIN::TryAcquire(
    TInstant now,
    const T_SEMAPHORE_SET_MIXIN::TProtoSemaphoreSetAcquire& control,
    const TTransactionCallContext& transactionCallContext)
{
    auto leaseUuid = control.lease_uuid();
    if (!leaseUuid) {
        leaseUuid = NClient::NObjects::GenerateUuid();
    }
    auto leaseDuration = CalculateLeaseDuration(control);
    auto newLeaseBudget = control.budget();
    if (!newLeaseBudget) {
        newLeaseBudget = 1;
    }
    YT_LOG_DEBUG("Acquiring semaphore set (Key: %v, LeaseUuid: %v, LeaseDuration: %v, Budget: %v)",
        this->GetKey(),
        leaseUuid,
        leaseDuration,
        newLeaseBudget);

    auto semaphores = TBase::Semaphores().Load();
    ScheduleSemaphoresLoad(semaphores);
    auto acquire = [now, &transactionCallContext, &leaseUuid, &leaseDuration] (
        TSemaphore* semaphore,
        ui64 leaseBudget)
    {
        T_SEMAPHORE_SET_MIXIN::TProtoSemaphoreAcquire semaphoreAcquireControl;
        semaphoreAcquireControl.set_lease_uuid(leaseUuid);
        semaphoreAcquireControl.set_duration(leaseDuration);
        semaphoreAcquireControl.set_budget(leaseBudget);
        return semaphore->TryAcquire(now, semaphoreAcquireControl, transactionCallContext);
    };

    bool leaseFound = false;
    ui64 acquiredBudget = 0;
    for (auto* semaphore : semaphores) {
        auto* lease = semaphore->template As<TSemaphore>()->TryGetFreshLease(now, leaseUuid);
        if (lease) {
            leaseFound = true;
            acquiredBudget += lease->budget();
            auto acquireResultOrError = acquire(semaphore->template As<TSemaphore>(), lease->budget());
            if (!acquireResultOrError.IsOK()) {
                return acquireResultOrError;
            }
        }
    }

    if (leaseFound) {
        if (acquiredBudget != newLeaseBudget) {
            return TError(
                "Changing the budget of existing lease %Qv of %v is not allowed: was %v, got %v",
                this->GetDisplayName(),
                leaseUuid,
                acquiredBudget,
                newLeaseBudget);
        }
        return TError{};
    }

    semaphores.erase(
        std::remove_if(semaphores.begin(), semaphores.end(), [now] (const auto* semaphore) {
            return semaphore->template As<TSemaphore>()->IsFull(now);
        }),
        semaphores.end());

    Shuffle(semaphores.begin(), semaphores.end());
    auto it = std::find_if(semaphores.begin(), semaphores.end(),
        [now, newLeaseBudget] (const auto* semaphore) {
            return semaphore->template As<TSemaphore>()->CalculateFreeBudget(now) >= newLeaseBudget;
        });
    if (it != semaphores.end()) {
        return acquire((*it)->template As<TSemaphore>(), newLeaseBudget);
    }

    while (newLeaseBudget && !semaphores.empty()) {
        auto* semaphore = semaphores.back();
        auto budgetToAcquire = std::min(newLeaseBudget, semaphore->template As<TSemaphore>()->CalculateFreeBudget(now));
        auto acquireResultOrError = acquire(semaphore->template As<TSemaphore>(), budgetToAcquire);
        if (!acquireResultOrError.IsOK()) {
            return acquireResultOrError;
        }
        newLeaseBudget -= budgetToAcquire;
        semaphores.pop_back();
    }

    if (newLeaseBudget) {
        return TError(
            NClient::EErrorCode::SemaphoreFull,
            "Semaphore set %Qv is full",
            this->GetKey());
    }
    return TError{};
}

template <
    class TGeneratedSemaphoreSet,
    NMpl::DerivedFromSpecializationOf<TSemaphoreMixin> TSemaphore,
    class TProtoSemaphoreSet,
    class TProtoSemaphore>
void T_SEMAPHORE_SET_MIXIN::Acquire(
    TInstant now,
    const T_SEMAPHORE_SET_MIXIN::TProtoSemaphoreSetAcquire& control,
    const TTransactionCallContext& transactionCallContext)
{
    TryAcquire(now, control, transactionCallContext)
        .ThrowOnError();
}

template <
    class TGeneratedSemaphoreSet,
    NMpl::DerivedFromSpecializationOf<TSemaphoreMixin> TSemaphore,
    class TProtoSemaphoreSet,
    class TProtoSemaphore>
void T_SEMAPHORE_SET_MIXIN::Ping(
    TInstant now,
    const T_SEMAPHORE_SET_MIXIN::TProtoSemaphoreSetPing& control,
    const TTransactionCallContext& transactionCallContext)
{
    NSemaphores::ValidateLeaseControl(control);
    auto leaseUuid = control.lease_uuid();
    auto leaseDuration = CalculateLeaseDuration(control);

    auto semaphores = TBase::Semaphores().Load();
    ScheduleSemaphoresLoad(semaphores);
    bool hasNonExpiredLease = false;
    for (auto* semaphore : semaphores) {
        if (semaphore->template As<TSemaphore>()->ContainsFreshLease(now, leaseUuid)) {
            hasNonExpiredLease = true;
            T_SEMAPHORE_SET_MIXIN::TProtoSemaphorePing semaphorePingControl;
            semaphorePingControl.set_lease_uuid(leaseUuid);
            semaphorePingControl.set_duration(leaseDuration);
            semaphore->template As<TSemaphore>()->Ping(now, semaphorePingControl, transactionCallContext);
        }
    }
    THROW_ERROR_EXCEPTION_UNLESS(hasNonExpiredLease,
        "Lease %Qv of %v is expired or unknown",
        leaseUuid,
        this->GetDisplayName());
}

template <
    class TGeneratedSemaphoreSet,
    NMpl::DerivedFromSpecializationOf<TSemaphoreMixin> TSemaphore,
    class TProtoSemaphoreSet,
    class TProtoSemaphore>
void T_SEMAPHORE_SET_MIXIN::Release(
    TInstant now,
    const T_SEMAPHORE_SET_MIXIN::TProtoSemaphoreSetRelease& control)
{
    NSemaphores::ValidateLeaseControl(control);
    const auto& leaseUuid = control.lease_uuid();
    auto semaphores = TBase::Semaphores().Load();
    ScheduleSemaphoresLoad(semaphores);
    for (auto* semaphore : semaphores) {
        if (semaphore->template As<TSemaphore>()->ContainsFreshLease(now, leaseUuid)) {
            T_SEMAPHORE_SET_MIXIN::TProtoSemaphoreRelease semaphoreReleaseControl;
            semaphoreReleaseControl.set_lease_uuid(leaseUuid);
            semaphore->template As<TSemaphore>()->Release(now, semaphoreReleaseControl);
        }
    }
}

template <
    class TGeneratedSemaphoreSet,
    NMpl::DerivedFromSpecializationOf<TSemaphoreMixin> TSemaphore,
    class TProtoSemaphoreSet,
    class TProtoSemaphore>
THashMap<TString, THashMap<TString, const typename T_SEMAPHORE_SET_MIXIN::TProtoSemaphoreLease>>
    T_SEMAPHORE_SET_MIXIN::GetFreshLeasesBySemaphore(TInstant now) const
{
    auto semaphores = TBase::Semaphores().Load();
    ScheduleSemaphoresLoad(semaphores);
    for (const auto& semaphore : semaphores) {
        semaphore->Status().Etc().ScheduleLoad();
    }

    THashMap<TString, THashMap<TString, const typename T_SEMAPHORE_SET_MIXIN::TProtoSemaphoreLease>> semaphoresToFreshLeases;
    for (const auto& semaphore : semaphores) {
        semaphoresToFreshLeases[semaphore->GetId()] = semaphore->template As<TSemaphore>()->GetFreshLeases(now);
    }
    return semaphoresToFreshLeases;
}

template <
    class TGeneratedSemaphoreSet,
    NMpl::DerivedFromSpecializationOf<TSemaphoreMixin> TSemaphore,
    class TProtoSemaphoreSet,
    class TProtoSemaphore>
template <class TGeneratedSemaphore>
void T_SEMAPHORE_SET_MIXIN::ScheduleSemaphoresLoad(const std::vector<TGeneratedSemaphore*>& semaphores)
{
    for (auto* semaphore : semaphores) {
        semaphore->Status().Etc().ScheduleLoad();
        semaphore->Spec().Etc().ScheduleLoad();
    }
}

////////////////////////////////////////////////////////////////////////////////

#undef T_SEMAPHORE_SET_MIXIN

} // namespace NYT::NOrm::NServer::NObjects
