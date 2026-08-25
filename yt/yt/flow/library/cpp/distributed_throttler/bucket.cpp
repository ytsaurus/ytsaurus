#include "bucket.h"

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow::NDistributedThrottler {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

bool TDistributedThrottlerBucket::TPendingRequest::operator>(const TPendingRequest& other) const
{
    if (Timestamp != other.Timestamp) {
        return Timestamp > other.Timestamp;
    }
    return EnqueueTime > other.EnqueueTime;
}

TDistributedThrottlerBucket::TDistributedThrottlerBucket(
    TDistributedThrottlerBucketConfigPtr config,
    TDuration drainPeriod,
    IInvokerPtr invoker,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
    : DrainPeriod_(drainPeriod)
    , Invoker_(std::move(invoker))
    , Logger(std::move(logger))
    , Profiler_(profiler)
    , TokenBucket_(CreateReconfigurableThroughputThrottler(
        config->Throttler,
        Logger,
        profiler))
    , UnknownClassRequestsCounter_(Profiler_.Counter("/unknown_class_requests"))
    , ClasslessRequestsCounter_(Profiler_.Counter("/classless_requests"))
    , Scheduler_(config->ClassWeights)
    , MaxGrantAmount_(config->MaxGrantAmount)
    , HasConfiguredClasses_(!config->ClassWeights.empty())
{
    EnsureClassQueue(DefaultQuotaClassId, 1.0);
    for (const auto& [classId, weight] : config->ClassWeights) {
        EnsureClassQueue(classId, weight);
    }
}

TDistributedThrottlerBucket::~TDistributedThrottlerBucket()
{
    Stop();
}

void TDistributedThrottlerBucket::Start()
{
    DrainLoopResult_ = BIND(&TDistributedThrottlerBucket::DrainLoop, MakeWeak(this))
        .AsyncVia(Invoker_)
        .Run();
}

TFuture<void> TDistributedThrottlerBucket::RequestQuota(
    const std::string& clientId,
    const TQuotaClassId& quotaClassId,
    i64 amount,
    ui64 timestamp)
{
    auto promise = NewPromise<void>();
    auto throttleHolder = std::make_shared<NThreading::TAtomicObject<TFuture<void>>>();
    // One handler per request, forwarding cancellation to whichever chunk is
    // in flight.
    promise.OnCanceled(BIND([throttleHolder] (const TError& error) {
        if (auto future = throttleHolder->Load()) {
            future.Cancel(error);
        }
    }));
    {
        auto guard = Guard(Lock_);
        const auto resolvedClassId = ResolveClassId(quotaClassId);
        auto& queue = GetOrCrash(ClassQueues_, resolvedClassId).Requests;
        queue.push(TPendingRequest{
            .Timestamp = timestamp,
            .EnqueueTime = TInstant::Now(),
            .ClientId = clientId,
            .Amount = amount,
            .Promise = promise,
            .ThrottleHolder = throttleHolder,
        });
        auto& classQueue = GetOrCrash(ClassQueues_, resolvedClassId);
        ++classQueue.PendingRequestCount;
        classQueue.PendingAmount += amount;
        UpdatePendingSensors(classQueue);
        Scheduler_.Activate(resolvedClassId, queue.top().Timestamp);
    }
    return promise.ToFuture();
}

void TDistributedThrottlerBucket::Reconfigure(TDistributedThrottlerBucketConfigPtr config)
{
    TokenBucket_->Reconfigure(config->Throttler);

    auto guard = Guard(Lock_);
    Scheduler_.Reconfigure(config->ClassWeights);
    MaxGrantAmount_ = config->MaxGrantAmount;
    HasConfiguredClasses_ = !config->ClassWeights.empty();
    EnsureClassQueue(DefaultQuotaClassId, 1.0).WeightGauge.Update(1.0);
    for (const auto& [classId, weight] : config->ClassWeights) {
        EnsureClassQueue(classId, weight).WeightGauge.Update(weight);
    }
    RemoveDrainedRetiredClasses();
}

void TDistributedThrottlerBucket::SetDrainPeriod(TDuration drainPeriod)
{
    DrainPeriod_.store(drainPeriod, std::memory_order::relaxed);
}

void TDistributedThrottlerBucket::Stop()
{
    if (DrainLoopResult_) {
        DrainLoopResult_.Cancel(TError("Throttler bucket stopped"));
    }
}

IReconfigurableThroughputThrottlerPtr TDistributedThrottlerBucket::GetTokenBucket() const
{
    return TokenBucket_;
}

TQuotaClassId TDistributedThrottlerBucket::ResolveClassId(
    const TQuotaClassId& quotaClassId)
{
    if (quotaClassId.empty()) {
        // Legitimate for a manually obtained throttler and for computations
        // that configure no class, but on a bucket with weighted classes it
        // also means someone competes in the default class — surface it.
        if (HasConfiguredClasses_) {
            ClasslessRequestsCounter_.Increment();
        }
        return DefaultQuotaClassId;
    }
    if (!Scheduler_.IsAccepting(quotaClassId)) {
        UnknownClassRequestsCounter_.Increment();
        return DefaultQuotaClassId;
    }
    return quotaClassId;
}

TDistributedThrottlerBucket::TClassQueue&
TDistributedThrottlerBucket::EnsureClassQueue(
    const TQuotaClassId& classId,
    double weight)
{
    auto [it, inserted] = ClassQueues_.try_emplace(classId);
    if (inserted) {
        auto profiler = Profiler_
            .WithTag("quota_class", classId)
            .WithPrefix("/quota_class");
        it->second.GrantedCounter = profiler.Counter("/granted");
        it->second.RefundedCounter = profiler.Counter("/refunded");
        it->second.PendingRequestsGauge = profiler.Gauge("/pending_requests");
        it->second.PendingAmountGauge = profiler.Gauge("/pending_amount");
        it->second.WaitTimeTimer = profiler.Timer("/wait_time");
        it->second.WeightGauge = profiler.Gauge("/weight");
    }
    it->second.WeightGauge.Update(weight);
    return it->second;
}

void TDistributedThrottlerBucket::UpdatePendingSensors(TClassQueue& classQueue)
{
    classQueue.PendingRequestsGauge.Update(classQueue.PendingRequestCount);
    classQueue.PendingAmountGauge.Update(classQueue.PendingAmount);
}

void TDistributedThrottlerBucket::RefreshClassActivity(const TQuotaClassId& classId)
{
    const auto& requests = GetOrCrash(ClassQueues_, classId).Requests;
    if (requests.empty()) {
        Scheduler_.Deactivate(classId);
    } else {
        Scheduler_.Activate(classId, requests.top().Timestamp);
    }
}

std::optional<TDistributedThrottlerBucket::TDispatch>
TDistributedThrottlerBucket::TryTakeDispatch()
{
    while (auto classId = Scheduler_.SelectClass()) {
        auto& classQueue = GetOrCrash(ClassQueues_, *classId);
        YT_VERIFY(!classQueue.Requests.empty());

        auto request = classQueue.Requests.top();
        classQueue.Requests.pop();

        if (request.Promise.IsCanceled()) {
            --classQueue.PendingRequestCount;
            classQueue.PendingAmount -= request.Amount;
            UpdatePendingSensors(classQueue);
            RefundGranted(classQueue, *classId, request);
            RefreshClassActivity(*classId);
            MaybeRemoveRetiredClass(*classId);
            continue;
        }

        const auto grantAmount = MaxGrantAmount_
            ? std::min(request.Amount, *MaxGrantAmount_)
            : request.Amount;
        const bool final = grantAmount == request.Amount;

        // Only the dispatched chunk leaves the pending gauges; the undispatched
        // remainder stays visible while the chunk waits on the token bucket.
        classQueue.PendingAmount -= grantAmount;
        if (final) {
            --classQueue.PendingRequestCount;
        }
        // The class stays active for the whole token-bucket wait, even once its
        // queue is empty. Deactivating here would let #MaybeReset zero the
        // virtual time while the lock is released, so the charge or refund that
        // #CompleteDispatch later applies would be measured against a baseline
        // that no longer exists — a refund would then leave phantom negative
        // credit and the class would monopolize the scheduler.
        Scheduler_.Activate(*classId, request.Timestamp);
        UpdatePendingSensors(classQueue);

        classQueue.InFlight = true;
        if (!request.Started) {
            classQueue.WaitTimeTimer.Record(TInstant::Now() - request.EnqueueTime);
            request.Started = true;
        }
        return TDispatch{
            .ClassId = *classId,
            .Request = std::move(request),
            .GrantAmount = grantAmount,
            .Final = final,
            .ChargeWeight = Scheduler_.GetWeight(*classId),
        };
    }

    return std::nullopt;
}

void TDistributedThrottlerBucket::CompleteDispatch(TDispatch& dispatch, const TError& error)
{
    {
        auto guard = Guard(Lock_);
        auto& classQueue = GetOrCrash(ClassQueues_, dispatch.ClassId);
        classQueue.InFlight = false;

        if (error.IsOK()) {
            classQueue.GrantedCounter.Increment(dispatch.GrantAmount);
            Scheduler_.Charge(dispatch.ClassId, dispatch.GrantAmount, dispatch.ChargeWeight);
            dispatch.Request.Granted += dispatch.GrantAmount;
            dispatch.Request.ChargedVirtualTime +=
                static_cast<double>(dispatch.GrantAmount) / dispatch.ChargeWeight;
        }

        if (!dispatch.Final) {
            if (error.IsOK() && !dispatch.Request.Promise.IsCanceled()) {
                // Requeue the remainder; it never left the pending gauges.
                dispatch.Request.Amount -= dispatch.GrantAmount;
                classQueue.Requests.push(dispatch.Request);
            } else {
                // The request dies mid-way: drop the remainder from the gauges
                // and refund the chunks it already consumed.
                classQueue.PendingAmount -= dispatch.Request.Amount - dispatch.GrantAmount;
                --classQueue.PendingRequestCount;
                UpdatePendingSensors(classQueue);
                RefundGranted(classQueue, dispatch.ClassId, dispatch.Request);
            }
        } else if (!error.IsOK()) {
            RefundGranted(classQueue, dispatch.ClassId, dispatch.Request);
        }

        // Deferred from TryTakeDispatch: now that the charge or refund has been
        // applied, the class may safely go idle and the scheduler may reset.
        RefreshClassActivity(dispatch.ClassId);
        MaybeRemoveRetiredClass(dispatch.ClassId);
    }

    if (error.IsOK()) {
        if (dispatch.Final) {
            dispatch.Request.Promise.TrySet();
        }
    } else {
        dispatch.Request.Promise.TrySet(error);
    }
}

void TDistributedThrottlerBucket::RefundGranted(
    TClassQueue& classQueue,
    const TQuotaClassId& classId,
    const TPendingRequest& request)
{
    if (request.Granted <= 0) {
        return;
    }
    // The request died before full delivery: return the tokens its granted
    // chunks consumed and roll back the class's virtual-time charge, so the
    // shared bucket is not double-charged when the client retries. The
    // rollback replays the charge that was actually applied — recomputing it
    // from the current weight would be asymmetric once a live reconfiguration
    // has changed that weight.
    TokenBucket_->Release(request.Granted);
    Scheduler_.ChargeVirtualTime(classId, -request.ChargedVirtualTime);
    classQueue.RefundedCounter.Increment(request.Granted);
}

void TDistributedThrottlerBucket::MaybeRemoveRetiredClass(const TQuotaClassId& classId)
{
    if (classId == DefaultQuotaClassId || !Scheduler_.IsRetired(classId)) {
        return;
    }
    const auto& classQueue = GetOrCrash(ClassQueues_, classId);
    if (!classQueue.Requests.empty() || classQueue.InFlight) {
        return;
    }
    Scheduler_.RemoveRetiredClass(classId);
    ClassQueues_.erase(classId);
}

void TDistributedThrottlerBucket::RemoveDrainedRetiredClasses()
{
    std::vector<TQuotaClassId> classIds;
    classIds.reserve(ClassQueues_.size());
    for (const auto& [classId, _] : ClassQueues_) {
        classIds.push_back(classId);
    }
    for (const auto& classId : classIds) {
        MaybeRemoveRetiredClass(classId);
    }
}

void TDistributedThrottlerBucket::DrainLoop(TWeakPtr<TDistributedThrottlerBucket> weakThis)
{
    while (true) {
        try {
            std::optional<TDispatch> dispatch;
            TDuration drainPeriod;
            {
                auto strongThis = weakThis.Lock();
                if (!strongThis) {
                    return;
                }
                auto guard = Guard(strongThis->Lock_);
                dispatch = strongThis->TryTakeDispatch();
                drainPeriod = strongThis->DrainPeriod_.load(std::memory_order::relaxed);
            }

            if (!dispatch) {
                WaitFor(TDelayedExecutor::MakeDelayed(drainPeriod)).ThrowOnError();
                continue;
            }

            auto strongThis = weakThis.Lock();
            if (!strongThis) {
                // The bucket died after the dispatch left the queue, so the
                // bookkeeping it belonged to is gone too. Resolve the request
                // explicitly rather than leaving the client to decode the
                // generic abandoned-promise error.
                dispatch->Request.Promise.TrySet(TError(
                    NYT::EErrorCode::Canceled,
                    "Throttler bucket was destroyed while the request was in flight"));
                return;
            }

            // Clear the holder and account for the dispatch however this
            // iteration ends, fiber cancellation included.
            bool completed = false;
            auto guard = Finally([&] {
                dispatch->Request.ThrottleHolder->Store(TFuture<void>());
                if (!completed) {
                    strongThis->CompleteDispatch(
                        *dispatch,
                        TError(NYT::EErrorCode::Canceled, "Throttler drain loop interrupted"));
                }
            });

            auto throttleFuture = strongThis->TokenBucket_->Throttle(dispatch->GrantAmount);
            // Publish the wait so cancelling the request releases the queued
            // amount instead of pinning the drain fiber.
            dispatch->Request.ThrottleHolder->Store(throttleFuture);
            // A cancellation racing the publish above would be lost otherwise.
            if (dispatch->Request.Promise.IsCanceled()) {
                throttleFuture.Cancel(TError(NYT::EErrorCode::Canceled, "Quota request canceled"));
            }
            auto error = WaitFor(throttleFuture);

            // Set first, so a throw below cannot complete the dispatch twice.
            completed = true;
            strongThis->CompleteDispatch(*dispatch, error);
        } catch (const std::exception& ex) {
            if (auto strongThis = weakThis.Lock()) {
                auto& Logger = strongThis->Logger;
                YT_TLOG_ERROR("Throttler drain loop failed")
                    .With(ex);
            } else {
                return;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
