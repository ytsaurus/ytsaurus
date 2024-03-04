#include "disk_location.h"

#include "config.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/misc/private.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/misc/fs.h>

namespace NYT::NDataNode {

using namespace NConcurrency;
using namespace NClusterNode;
using namespace NYTree;
using namespace NYson;

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

const TString& TDiskLocation::GetId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Id_;
}

TDiskLocationConfigPtr TDiskLocation::GetRuntimeConfig() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return RuntimeConfig_.Acquire();
}

void TDiskLocation::Reconfigure(TDiskLocationConfigPtr config)
{
    VERIFY_THREAD_AFFINITY_ANY();

    RuntimeConfig_.Store(std::move(config));
}

bool TDiskLocation::IsEnabled() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto value = State_.load();
    return value == ELocationState::Enabled;
}

bool TDiskLocation::CanHandleIncomingActions() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto currentState = State_.load();
    return currentState == ELocationState::Enabled ||
        currentState == ELocationState::Enabling;
}

ELocationState TDiskLocation::GetState() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return State_.load();
}

bool TDiskLocation::ChangeState(
    ELocationState newState,
    std::optional<ELocationState> expectedState)
{
    VERIFY_THREAD_AFFINITY_ANY();

    StateChangingLock_.AcquireWriter();

    auto finally = Finally([=, this, this_ = MakeStrong(this)] {
        StateChangingLock_.ReleaseWriter();
    });

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

    State_.store(newState);
    return true;
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
            THROW_ERROR_EXCEPTION("Minimum disk space requirement is not met for location %Qv",
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

} // namespace NYT
