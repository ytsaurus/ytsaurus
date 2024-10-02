#ifndef SEMAPHORE_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include semaphore_detail.h"
// For the sake of sane code completion.
#include "semaphore_detail.h"
#endif

#include "object_reflection.h"
#include "private.h"
#include "transaction.h"

namespace NYT::NOrm::NServer::NObjects {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <class TSemaphoreStatus>
ui64 CalculateAcquiredBudget(TInstant now, const TSemaphoreStatus& status)
{
    const auto& leases = status.leases();
    ui64 acquiredBudget = 0;
    for (const auto& [_, lease] : leases) {
        if (now.MilliSeconds() < lease.expiration_time()) {
            acquiredBudget += lease.budget();
        }
    }
    return acquiredBudget;
}

template <class TLease, class TControl>
void RefreshLease(
    TInstant now,
    TLease& lease,
    const TControl& control,
    const TTransactionCallContext& transactionCallContext,
    const TString& semaphoreName)
{
    lease.set_last_acquire_time(now.MilliSeconds());
    lease.set_expiration_time(
        std::max(now.MilliSeconds() + control.duration(), lease.expiration_time()));
    lease.set_acquirer_endpoint_description(ToProto<TProtobufString>(transactionCallContext.CallerEndpointDescription));
    YT_LOG_DEBUG("Acquired lease (Semaphore: %v, LeaseUuid: %v, ExpirationTime: %v)",
        semaphoreName,
        control.lease_uuid(),
        lease.expiration_time());
}

template <class TSemaphoreStatus>
void RemoveExpiredLeases(TInstant now, TSemaphoreStatus& status, const TString& semaphoreName)
{
    auto& leases = *status.mutable_leases();
    for (auto it = leases.begin(); it != leases.end();) {
        if (now.MilliSeconds() >= it->second.expiration_time()) {
            YT_LOG_DEBUG("Clearing expired lease (Semaphore: %v, LeaseUuid: %v)",
                semaphoreName,
                it->first);
            it = leases.erase(it);
        } else {
            ++it;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

#define T_SEMAPHORE_MIXIN TSemaphoreMixin<TGeneratedSemaphore, TProtoSemaphore>

////////////////////////////////////////////////////////////////////////////////

template <class TGeneratedSemaphore, class TProtoSemaphore>
NYT::TError T_SEMAPHORE_MIXIN::TryAcquire(
    TInstant now,
    const T_SEMAPHORE_MIXIN::TProtoSemaphoreAcquire& control,
    const TTransactionCallContext& transactionCallContext)
{
    return NSemaphores::TryAcquire(
        now,
        TBase::Spec().Etc().Load(),
        *TBase::Status().Etc().MutableLoad(),
        control,
        transactionCallContext,
        this->GetId());
}

template <class TGeneratedSemaphore, class TProtoSemaphore>
void T_SEMAPHORE_MIXIN::Acquire(
    TInstant now,
    const T_SEMAPHORE_MIXIN::TProtoSemaphoreAcquire& control,
    const TTransactionCallContext& transactionCallContext)
{
    TryAcquire(now, control, transactionCallContext)
        .ThrowOnError();
}

template <class TGeneratedSemaphore, class TProtoSemaphore>
void T_SEMAPHORE_MIXIN::Ping(
    TInstant now,
    const T_SEMAPHORE_MIXIN::TProtoSemaphorePing& control,
    const TTransactionCallContext& transactionCallContext)
{
    NSemaphores::Ping(
        now,
        *TBase::Status().Etc().MutableLoad(),
        control,
        transactionCallContext,
        this->GetId());
}

template <class TGeneratedSemaphore, class TProtoSemaphore>
void T_SEMAPHORE_MIXIN::Release(
    TInstant now,
    const T_SEMAPHORE_MIXIN::TProtoSemaphoreRelease& control)
{
    NSemaphores::Release(
        now,
        *TBase::Status().Etc().MutableLoad(),
        control,
        this->GetId());
}

template <class TGeneratedSemaphore, class TProtoSemaphore>
THashMap<TString, const typename T_SEMAPHORE_MIXIN::TProtoSemaphoreLease> T_SEMAPHORE_MIXIN::GetFreshLeases(TInstant now) const
{
    const auto& leases = TBase::Status().Etc().Load().leases();
    THashMap<TString, const typename T_SEMAPHORE_MIXIN::TProtoSemaphoreLease> freshLeases;
    for (const auto& [leaseUuid, lease] : leases) {
        if (now.MilliSeconds() < lease.expiration_time()) {
            freshLeases.emplace(leaseUuid, lease);
        }
    }
    return freshLeases;
}

template <class TGeneratedSemaphore, class TProtoSemaphore>
const typename T_SEMAPHORE_MIXIN::TProtoSemaphoreLease* T_SEMAPHORE_MIXIN::TryGetFreshLease(
    TInstant now,
    const TString& leaseUuid) const
{
    const auto& leases = TBase::Status().Etc().Load().leases();
    auto it = leases.find(leaseUuid);
    if (it != leases.end() && now.MilliSeconds() < it->second.expiration_time()) {
        return &it->second;
    }
    return nullptr;
}

template <class TGeneratedSemaphore, class TProtoSemaphore>
void T_SEMAPHORE_MIXIN::RemoveExpiredLeases(TInstant now)
{
    NObjects::RemoveExpiredLeases(now, *TBase::Status().Etc().MutableLoad(), this->GetId());
}

template <class TGeneratedSemaphore, class TProtoSemaphore>
bool T_SEMAPHORE_MIXIN::ContainsFreshLease(TInstant now, const TString& leaseUuid) const
{
    return TryGetFreshLease(now, leaseUuid);
}

template <class TGeneratedSemaphore, class TProtoSemaphore>
bool T_SEMAPHORE_MIXIN::ContainsFreshLease(TInstant now) const
{
    const auto& leases = TBase::Status().Etc().Load().leases();

    return std::any_of(leases.begin(), leases.end(), [now] (const auto& leaseEntry) {
        return leaseEntry.second.expiration_time() >= now.MilliSeconds();
    });
}

template <class TGeneratedSemaphore, class TProtoSemaphore>
ui64 T_SEMAPHORE_MIXIN::LoadBudget() const
{
    return TBase::Spec().Etc().Load().budget();
}

template <class TGeneratedSemaphore, class TProtoSemaphore>
ui64 T_SEMAPHORE_MIXIN::CalculateFreeBudget(TInstant now) const
{
    return std::max<i64>(
        0,
        static_cast<i64>(LoadBudget())
        - static_cast<i64>(CalculateAcquiredBudget(now, TBase::Status().Etc().Load())));
}

template <class TGeneratedSemaphore, class TProtoSemaphore>
bool T_SEMAPHORE_MIXIN::IsFull(TInstant now) const
{
    return CalculateAcquiredBudget(now, TBase::Status().Etc().Load()) >= LoadBudget();
}

#undef T_SEMAPHORE_MIXIN

namespace NSemaphores {

////////////////////////////////////////////////////////////////////////////////

template <class TControl>
void ValidateLeaseControl(const TControl& control)
{
    THROW_ERROR_EXCEPTION_IF(control.lease_uuid().empty(),
        "No lease UUID was provided in control");
}

template <class TSemaphoreSpec, class TSemaphoreStatus, class TSemaphoreAcquire>
TError TryAcquire(
    TInstant now,
    const TSemaphoreSpec& spec,
    TSemaphoreStatus& status,
    const TSemaphoreAcquire& control,
    const TTransactionCallContext& transactionCallContext,
    const TString& semaphoreName)
{
    ValidateLeaseControl(control);
    RemoveExpiredLeases(now, status, semaphoreName);
    const auto& leaseUuid = control.lease_uuid();

    auto acquiredBudget = CalculateAcquiredBudget(now, status);

    auto newLeaseBudget = control.budget();
    if (!newLeaseBudget) {
        newLeaseBudget = 1;
    }

    ui64 specBudget = spec.budget();

    auto& leases = *status.mutable_leases();
    auto it = leases.find(leaseUuid);
    if (it == leases.end()) {
        if (acquiredBudget + newLeaseBudget > specBudget) {
            return NYT::TError(
                NClient::EErrorCode::SemaphoreFull,
                "Semaphore %v is full: acquired budget is %v, total budget is %v, "
                "trying to acquire %v",
                semaphoreName,
                acquiredBudget,
                specBudget,
                newLeaseBudget);
        }
        auto& lease = leases[leaseUuid];
        lease.set_budget(newLeaseBudget);
        RefreshLease(now, lease, control, transactionCallContext, semaphoreName);
    } else {
        if (newLeaseBudget != it->second.budget()) {
            return NYT::TError(
                "Changing the budget of existing lease %v of semaphore %v is not allowed: "
                "was %v, trying to acquire %v",
                leaseUuid,
                semaphoreName,
                it->second.budget(),
                newLeaseBudget);
        }
        RefreshLease(now, it->second, control, transactionCallContext, semaphoreName);
    }
    return NYT::TError{};
}

template <class TSemaphoreStatus, class TSemaphorePing>
void Ping(
    TInstant now,
    TSemaphoreStatus& status,
    const TSemaphorePing& control,
    const TTransactionCallContext& transactionCallContext,
    const TString& semaphoreName)
{
    ValidateLeaseControl(control);
    RemoveExpiredLeases(now, status, semaphoreName);
    const auto& leaseUuid = control.lease_uuid();

    auto& leases = *status.mutable_leases();
    auto it = leases.find(leaseUuid);
    if (it == leases.end()) {
        THROW_ERROR_EXCEPTION("Lease %v of semaphore %v is expired or unknown",
            leaseUuid,
            semaphoreName);
    } else {
        RefreshLease(now, it->second, control, transactionCallContext, semaphoreName);
    }
}

template <class TSemaphoreStatus, class TSemaphoreRelease>
void Release(
    TInstant now,
    TSemaphoreStatus& status,
    const TSemaphoreRelease& control,
    const TString& semaphoreName)
{
    ValidateLeaseControl(control);
    RemoveExpiredLeases(now, status, semaphoreName);
    const auto& leaseUuid = control.lease_uuid();

    auto& leases = *status.mutable_leases();
    auto it = leases.find(leaseUuid);
    if (it != leases.end()) {
        YT_LOG_DEBUG("Releasing lease (Semaphore: %v, LeaseUuid: %v)",
            semaphoreName,
            leaseUuid);
        leases.erase(it);
    }
}

template <class TEmbeddedSemaphore, class TSemaphoreAcquire>
void EmbeddedAcquire(
    TTransaction* transaction,
    TObject* object,
    const TSemaphoreAcquire& control,
    const TTransactionCallContext& transactionCallContext)
{
    auto now = Now();
    auto embeddedSemaphore =
        GetScalarValueOrThrow<TEmbeddedSemaphore>(transaction, object, control.embedded_path());
    const auto& spec = embeddedSemaphore.spec();
    auto& status = *embeddedSemaphore.mutable_status();
    TString semaphoreName = Format("%v in %v",
        control.embedded_path(),
        object->GetDisplayName());

    TryAcquire(now, spec, status, control, transactionCallContext, semaphoreName).ThrowOnError();

    SetScalarValueOrThrow(
        transaction,
        object,
        control.embedded_path(),
        embeddedSemaphore,
        /*recursive*/ true,
        /*sharedWrite*/ std::nullopt,
        /*aggregateMode*/ EAggregateMode::Unspecified,
        transactionCallContext);
}

template <class TEmbeddedSemaphore, class TSemaphorePing>
void EmbeddedPing(
    TTransaction* transaction,
    TObject* object,
    const TSemaphorePing& control,
    const TTransactionCallContext& transactionCallContext)
{
    auto now = Now();
    auto embeddedSemaphore =
        GetScalarValueOrThrow<TEmbeddedSemaphore>(transaction, object, control.embedded_path());
    auto& status = *embeddedSemaphore.mutable_status();
    TString semaphoreName = Format("%v in %v",
        control.embedded_path(),
        object->GetDisplayName());

    Ping(now, status, control, transactionCallContext, semaphoreName);

    SetScalarValueOrThrow(
        transaction,
        object,
        control.embedded_path(),
        embeddedSemaphore,
        /*recursive*/ true,
        /*sharedWrite*/ std::nullopt,
        /*aggregateMode*/ EAggregateMode::Unspecified,
        transactionCallContext);
}

template <class TEmbeddedSemaphore, class TSemaphoreRelease>
void EmbeddedRelease(
    TTransaction* transaction,
    TObject* object,
    const TSemaphoreRelease& control,
    const TTransactionCallContext& transactionCallContext)
{
    auto now = Now();
    auto embeddedSemaphore =
        GetScalarValueOrThrow<TEmbeddedSemaphore>(transaction, object, control.embedded_path());
    auto& status = *embeddedSemaphore.mutable_status();
    TString semaphoreName = Format("%v in %v",
        control.embedded_path(),
        object->GetDisplayName());

    Release(now, status, control, semaphoreName);

    SetScalarValueOrThrow(
        transaction,
        object,
        control.embedded_path(),
        embeddedSemaphore,
        /*recursive*/ true,
        /*sharedWrite*/ std::nullopt,
        /*aggregateMode*/ EAggregateMode::Unspecified,
        transactionCallContext);
}

} // namespace NSemaphores

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
