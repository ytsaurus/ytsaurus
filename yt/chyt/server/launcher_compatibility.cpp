#include "launcher_compatibility.h"

#include "config.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ClickHouseYtLogger;
static constexpr int MinLauncherVersion = 0;

////////////////////////////////////////////////////////////////////////////////

void ValidateLauncherCompatibility(TLauncherConfigPtr launcherConfig)
{
    YT_TLOG_INFO("Validating launcher compatibility")
        .With("LauncherVersion", launcherConfig->Version)
        .With("MinLauncherVersion", MinLauncherVersion);
    if (launcherConfig->Version < MinLauncherVersion) {
        THROW_ERROR_EXCEPTION(
            "Launcher version is too old for setting up clique with this ytserver-clickhouse binary: "
            "expected launcher of version %v or newer, actual version is %v",
            MinLauncherVersion,
            launcherConfig->Version);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
