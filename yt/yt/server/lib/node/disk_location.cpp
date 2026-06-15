#include "disk_location.h"

#include "config.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NNode {

using namespace NConcurrency;
using namespace NClusterNode;
using namespace NProfiling;
using namespace NYTree;
using namespace NYson;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

class TDiskLocation::TProfiling
    : public TRefCounted
{
public:
    explicit TProfiling(const TProfiler& profiler)
        : Profiler_(profiler
            .WithPrefix("/disk_locations")
            .WithSparse())
        , Total_(Profiler_.Gauge("/total"))
    {
        Total_.Update(1);
    }

    void OnDisabled(const TError& disableReason)
    {
        auto disableMeta = ToString(disableReason.GetCode());
        auto guard = Guard(Lock_);

        if (!DisableSensors_.contains(disableMeta)) {
            auto taggedProfiler = Profiler_.WithTag("error_code", disableMeta);
            DisableSensors_[disableMeta] = std::pair{
                taggedProfiler.Gauge("/disabled"),
                taggedProfiler.Counter("/disable_rate"),
            };
        }

        auto& [gauge, counter] = GetOrCrash(DisableSensors_, disableMeta);
        gauge.Update(1);
        counter.Increment(1);

        YT_VERIFY(!CurrentDisableMeta_.has_value());
        CurrentDisableMeta_ = std::move(disableMeta);
    }

    void OnNoLongerDisabled()
    {
        YT_VERIFY(CurrentDisableMeta_.has_value());

        auto& [gauge, counter] = GetOrCrash(DisableSensors_, *CurrentDisableMeta_);
        gauge.Update(0);
        CurrentDisableMeta_.reset();
    }

private:
    const TProfiler Profiler_;
    const TGauge Total_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<std::string, std::pair<TGauge, TCounter>> DisableSensors_;
    std::optional<std::string> CurrentDisableMeta_;
};

////////////////////////////////////////////////////////////////////////////////

TDiskLocation::TDiskLocation(
    TDiskLocationConfigPtr config,
    TString id,
    const NLogging::TLogger& logger)
    : Id_(std::move(id))
    , Logger(logger.WithTag("LocationId: %v", Id_))
    , StaticConfig_(std::move(config))
    , RuntimeConfig_(StaticConfig_)
{ }

TDiskLocation::~TDiskLocation() = default;

const TString& TDiskLocation::GetId() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Id_;
}

TDiskLocationConfigPtr TDiskLocation::GetRuntimeConfig() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return RuntimeConfig_.Acquire();
}

void TDiskLocation::Reconfigure(TDiskLocationConfigPtr config)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    RuntimeConfig_.Store(std::move(config));

    if (GetRuntimeConfig()->DisableProfiling) {
        Profiling_ = nullptr;
    } else if (!Profiling_.Acquire()) {
        YT_VERIFY(Profiler_.has_value());
        InitializeDiskLocationProfiling(*Profiler_);
    }
}

bool TDiskLocation::IsEnabled() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto value = State_.load();
    return value == ELocationState::Enabled;
}

bool TDiskLocation::CanHandleIncomingActions() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto currentState = State_.load();
    return currentState == ELocationState::Enabled ||
        currentState == ELocationState::Enabling;
}

ELocationState TDiskLocation::GetState() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return State_.load();
}

bool TDiskLocation::ChangeState(
    ELocationState newState,
    std::optional<ELocationState> expectedState,
    const TError& disableReason)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (!disableReason.IsOK()) {
        YT_LOG_FATAL_UNLESS(
            newState == ELocationState::Disabled,
            disableReason,
            "Disable reason provided for a non-disable state (NewState: %v)",
            newState);
    }

    auto guard = NThreading::WriterGuard(StateChangingLock_);

    if (expectedState) {
        ELocationState currentState = State_.load();

        if (expectedState != currentState) {
            YT_LOG_WARNING(
                "Incompatible location state (Expected: %v, Actual: %v)",
                expectedState,
                currentState);
            return false;
        }
    }

    auto oldState = State_.exchange(newState);

    if (auto profiling = Profiling_.Acquire()) {
        if (oldState == ELocationState::Disabled) {
            profiling->OnNoLongerDisabled();
        }
        if (newState == ELocationState::Disabled) {
            profiling->OnDisabled(disableReason);
        }
    }

    return true;
}

void TDiskLocation::InitializeDiskLocationProfiling(const NProfiling::TProfiler& profiler)
{
    if (!Profiler_.has_value()) {
        Profiler_ = profiler;
    }

    if (GetRuntimeConfig()->DisableProfiling) {
        return;
    }

    auto guard = NThreading::ReaderGuard(StateChangingLock_);

    YT_VERIFY(!Profiling_);
    Profiling_ = New<TProfiling>(profiler);
    if (State_.load() == ELocationState::Disabled) {
        Profiling_.Acquire()->OnDisabled({});
    }
}

void TDiskLocation::ValidateLockFile() const
{
    YT_LOG_INFO("Checking lock file");

    auto lockFilePath = NFS::CombinePaths(StaticConfig_->Path, DisabledLockFileName);
    if (!NFS::Exists(lockFilePath)) {
        return;
    }

    TFile file(lockFilePath, OpenExisting | RdOnly | Seq | CloseOnExec);
    TFileInput fileInput(file);

    auto errorData = fileInput.ReadAll();
    if (errorData.empty()) {
        THROW_ERROR_EXCEPTION("Empty lock file found")
            << TErrorAttribute("lock_file", lockFilePath);
    }

    TError error;
    try {
        error = ConvertTo<TError>(TYsonString(errorData));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing lock file contents")
            << TErrorAttribute("lock_file", lockFilePath)
            << ex;
    }
    THROW_ERROR_EXCEPTION("Lock file found")
        << TErrorAttribute("lock_file", lockFilePath)
        << error;
}

void TDiskLocation::ValidateMinimumSpace() const
{
    YT_LOG_INFO("Checking minimum space");

    auto config = GetRuntimeConfig();
    if (config->MinDiskSpace) {
        auto minSpace = *config->MinDiskSpace;
        auto totalSpace = GetTotalSpace();
        if (totalSpace < minSpace) {
            THROW_ERROR_EXCEPTION(
                "Minimum disk space requirement is not met for location %Qv",
                Id_)
                << TErrorAttribute("actual_space", totalSpace)
                << TErrorAttribute("required_space", minSpace);
        }
    }
}

i64 TDiskLocation::GetTotalSpace() const
{
    auto statistics = NFS::GetDiskSpaceStatistics(StaticConfig_->Path);
    return statistics.TotalSpace;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNode
