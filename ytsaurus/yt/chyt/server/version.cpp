#include "version.h"

#include <build/scripts/c_templates/svnversion.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

TString GetCHYTVersion()
{
    TString branch = ::GetBranch();
    auto pos = branch.find("chyt/");
    TString hash(::GetProgramHash());
    if (hash.size() >= 10) {
        hash.resize(10);
    }
    if (pos != TString::npos) {
        pos += 5;
        return branch.substr(pos) + "." + ToString(::GetArcadiaPatchNumber()) + "~" + hash;
    } else if (branch.find("trunk") != TString::npos) {
        return "0.0." + ToString(::GetProgramSvnRevision()) + "-trunk~" + hash;
    } else {
        return "0.0." + ToString(::GetProgramSvnRevision()) + "-local~" + hash;
    }
}

////////////////////////////////////////////////////////////////////////////////

}
