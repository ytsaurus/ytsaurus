#include "buildinfo.h"

#include <library/cpp/svnversion/svnversion.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

TBuildInfo TBuildInfo::GetDefault()
{
    TBuildInfo buildInfo;

    buildInfo.BuildType = YTPROF_BUILD_TYPE;
    buildInfo.BuildType.to_lower(); // no shouting

    if (GetVCS() == TString{"arc"}) {
        buildInfo.ArcRevision = GetProgramCommitId();
    }

    return buildInfo;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
