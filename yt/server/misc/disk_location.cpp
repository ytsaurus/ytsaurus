#include "disk_location.h"

#include "config.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/data_node/master_connector.h>

#include <yt/core/yson/string.h>

#include <yt/core/misc/fs.h>

namespace NYT {

using namespace NCellNode;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TDiskLocation::TDiskLocation(
    TDiskLocationConfigPtr config,
    const TString& id,
    const NLogging::TLogger& logger)
    : Logger(NLogging::TLogger(logger)
         .AddTag("LocationId: %v", id))
    , Config_(config)
{ }

bool TDiskLocation::IsEnabled() const
{
    return Enabled_.load(); 
}

void TDiskLocation::ValidateLockFile() const
{
    LOG_INFO("Checking lock file");

    auto lockFilePath = NFS::CombinePaths(Config_->Path, DisabledLockFileName);
    if (!NFS::Exists(lockFilePath)) {
        return;
    }

    TFile file(lockFilePath, OpenExisting | RdOnly | Seq | CloseOnExec);
    TBufferedFileInput fileInput(file);

    auto errorData = fileInput.ReadAll();
    if (errorData.Empty()) {
        THROW_ERROR_EXCEPTION("Empty lock file found");
    }

    TError error;
    try {
        error = ConvertTo<TError>(TYsonString(errorData));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing lock file contents")
            << ex;
    }
    THROW_ERROR error;
}

void TDiskLocation::ValidateMinimumSpace() const
{
    LOG_INFO("Checking minimum space");

    if (Config_->MinDiskSpace) {
        i64 minSpace = *Config_->MinDiskSpace;
        i64 totalSpace = GetTotalSpace();
        if (totalSpace < minSpace) {
            THROW_ERROR_EXCEPTION("Minimum disk space requirement is not met") 
                << TErrorAttribute("actual_space", totalSpace) 
                << TErrorAttribute("required_space", minSpace);
        }
    }
}

i64 TDiskLocation::GetTotalSpace() const
{
    auto statistics = NFS::GetDiskSpaceStatistics(Config_->Path);
    return statistics.TotalSpace;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
