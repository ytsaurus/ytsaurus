#include "ya_version.h"

#include <yt/build/ya_version_data.h>

#include <util/stream/str.h>

#include <util/system/compiler.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TString CreateYTVersion(int major, int minor, TStringBuf branch)
{

#if defined(ARCADIA_PATCH_NUMBER)
    auto patch = ARCADIA_PATCH_NUMBER;
#else
    auto patch = 0;
#endif

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

    TString commit = ARCADIA_SOURCE_REVISION;
    TString buildUser = BUILD_USER;

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
    return BUILD_HOST;
}

TString GetYaBuildDate()
{
    return BUILD_DATE;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
