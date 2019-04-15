#include "program_cgroup_mixin.h"

#include <yt/ytlib/cgroup/cgroup.h>

#include <yt/core/logging/log.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Cgroup");

////////////////////////////////////////////////////////////////////////////////

TProgramCgroupMixin::TProgramCgroupMixin(NLastGetopt::TOpts& opts)
{
    opts
        .AddLongOption("cgroup", "join group upon starting a process")
        .Handler1T<TString>([&] (const TString& arg) {
            CgroupPaths_.push_back(arg);
        })
        .RequiredArgument("CGROUP_PATH");
}

bool TProgramCgroupMixin::HandleCgroupOptions()
{
    try {
        for (const auto& cgroupPath : CgroupPaths_) {
            NCGroup::TNonOwningCGroup cgroup(cgroupPath);
            cgroup.EnsureExistance();
            cgroup.AddCurrentTask();
        }
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to handle cgroups");
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

