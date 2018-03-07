#include "disk_location.h"

#include "config.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/data_node/master_connector.h>

#include <yt/core/misc/fs.h>

namespace NYT {

using namespace NCellNode;

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
