#include "ya_version.h"

#include <build/scripts/c_templates/svnversion.h>

#include <util/stream/format.h>
#include <util/stream/str.h>

#include <util/system/compiler.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TString CreateYTVersion(int major, int minor, int patch, TStringBuf branch)
{
    TStringStream out;
    out << major << "." << minor << "." << patch;
    out << "-" << branch;
    out << "-ya";

#if !defined(NDEBUG)
    out << "debug";
#endif

#if defined(_asan_enabled_)
    out << "-asan";
#endif

    TString commit = GetProgramHash();
    if (commit.empty()) {
        commit = GetProgramCommitId();
    }
    TString buildUser = GetProgramBuildUser();

    // When we use `ya make --dist` distbuild makes mess instead of svn revision:
    //   BUILD_USER == "Unknown user"
    //   ARCADIA_SOURCE_REVISION = "-1"
    // When Hermes looks at such horrors it goes crazy.
    // Here are some hacks to help Hermes keep its saninty.
    if (commit == "-1") {
        commit = TString(20, '0');
    }
    if (buildUser == "Unknown user") {
        buildUser = "distbuild";
    }

    out << "~" << commit.substr(0, 10);
    if (buildUser != "teamcity") {
        out << "+" << buildUser;
    }

    return out.Str();
}

TString GetYaHostName()
{
    return GetProgramBuildHost();
}

TString GetYaBuildDate()
{
    return GetProgramBuildDate();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
