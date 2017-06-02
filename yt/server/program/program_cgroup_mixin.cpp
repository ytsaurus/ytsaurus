#include "program_cgroup_mixin.h"

#include <yt/ytlib/cgroup/cgroup.h>

namespace NYT {

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
    for (const auto& cgroupPath : CgroupPaths_) {
        NCGroup::TNonOwningCGroup cgroup(cgroupPath);
        cgroup.EnsureExistance();
        cgroup.AddCurrentTask();
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

