#include "version.h"

#include <build/scripts/c_templates/svnversion.h>

#include <yt/build/build.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

TString GetVersion()
{
    TString branch = GetBranch();
    auto pos = branch.find("chyt/");
    if (pos != TString::npos) {
        pos += 5;
        return branch.substr(pos) + "." + ToString(GetVersionPatch()) + "~" + GetProgramCommitId();
    } else if (branch.find("trunk") != TString::npos) {
        return "0.0." + ToString(GetProgramSvnRevision()) + "~" + GetProgramCommitId();
    } else {
        return "0.0." + ToString(GetProgramSvnRevision()) + "-local~" + GetProgramCommitId();
    }
}

////////////////////////////////////////////////////////////////////////////////

}
