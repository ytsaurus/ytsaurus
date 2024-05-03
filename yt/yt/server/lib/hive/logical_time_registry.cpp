#include "logical_time_registry.h"

#include <yt/yt/server/lib/hydra/hydra_manager.h>
#include <yt/yt/server/lib/hydra/serialize.h>
#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/server/lib/hive/config.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NHiveServer {

using namespace NConcurrency;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TLogicalTimeRegistry::TLogicalTimeRegistry(
    TLogicalTimeRegistryConfigPtr config,
    IInvokerPtr automatonInvoker,
    IHydraManagerPtr hydraManager,
    const NProfiling::TProfiler& profiler)
        : Config_(std::move(config))
        , AutomatonInvoker_(std::move(automatonInvoker))
        , HydraManager_(std::move(hydraManager))
{
    EvictionExecutor_ = New<TPeriodicExecutor>(
        AutomatonInvoker_,
        BIND(&TLogicalTimeRegistry::OnEvict, MakeWeak(this)),
        Config_->EvictionPeriod);
    EvictionExecutor_->Start();

    profiler.AddFuncGauge("/logical_time_registry/registry_size", MakeStrong(this), [this] {
        return TimeInfoMapSize_.load();
    });

    Clock_.SubscribeTick(BIND(&TLogicalTimeRegistry::OnTick, MakeWeak(this)));
}

TLogicalTime TLogicalTimeRegistry::TLamportClock::Tick(TLogicalTime externalTime)
{
    // COMPAT(danilalexeev)
    auto reign = GetCurrentMutationContext()->Request().Reign;
    // ETabletReign::HiveManagerLamportTimestamp = 100909.
    if (reign >= 100000 && reign < 100909) {
        return {};
    }

    Time_ = std::max(Time_, externalTime);
    auto time = TLogicalTime(++Time_.Underlying());
    Tick_.Fire(time);
    return time;
}

void TLogicalTimeRegistry::TLamportClock::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, Time_);
}

void TLogicalTimeRegistry::TLamportClock::Load(TLoadContext& context)
{
    using NYT::Load;
    Load(context, Time_);
}

TLogicalTimeRegistry::TLamportClock* TLogicalTimeRegistry::GetClock()
{
    return &Clock_;
}

std::pair<TLogicalTime, TConsistentState> TLogicalTimeRegistry::GetConsistentState(std::optional<TLogicalTime> logicalTime)
{
    if (TimeInfoMap_.empty()) {
        THROW_ERROR_EXCEPTION(
            NHiveClient::EErrorCode::TimeEntryNotFound,
            "Logical time registry is empty");
    }

    auto currentState = TConsistentState{
        .SequenceNumber = HydraManager_->GetSequenceNumber(),
        .SegmentId = HydraManager_->GetAutomatonVersion().SegmentId,
    };
    if (!logicalTime) {
        auto it = TimeInfoMap_.rbegin();
        return {it->first, currentState};
    }

    auto it = TimeInfoMap_.upper_bound(*logicalTime);
    if (it == TimeInfoMap_.begin()) {
        THROW_ERROR_EXCEPTION(
            NHiveClient::EErrorCode::TimeEntryNotFound,
            "Logical time entry has been evicted");
    }

    auto resultState = it != TimeInfoMap_.end()
        ? it->second.AdjustedState
        : currentState;

    if (resultState.SequenceNumber < 0 || resultState.SegmentId < 0) {
        THROW_ERROR_EXCEPTION(
            NHiveClient::EErrorCode::TimeEntryNotFound,
            "Logical time entry is invalid");
    }

    return {std::prev(it)->first, resultState};
}

void TLogicalTimeRegistry::OnTick(TLogicalTime logicalTime)
{
    auto* mutationContext = GetCurrentMutationContext();
    auto version = mutationContext->GetVersion();

    EmplaceOrCrash(TimeInfoMap_,
        logicalTime,
        TTimeInfo{
            .AdjustedState = TConsistentState{
                .SequenceNumber = mutationContext->GetSequenceNumber() - 1,
                .SegmentId = version.RecordId == 0
                    ? version.SegmentId - 1
                    : version.SegmentId,
            },
            .Timestamp = mutationContext->GetTimestamp(),
        });
    ++TimeInfoMapSize_;
}

void TLogicalTimeRegistry::OnEvict()
{
    auto threshold = TInstant::Now() - Config_->ExpirationTimeout;

    while (!TimeInfoMap_.empty()) {
        auto it = TimeInfoMap_.begin();
        if (it->second.Timestamp > threshold) {
            break;
        }
        TimeInfoMap_.erase(it);
    }

    TimeInfoMapSize_ = std::ssize(TimeInfoMap_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
