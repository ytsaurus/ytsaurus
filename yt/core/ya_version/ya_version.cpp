#include "ya_version.h"

#include <yt/core/ya_version/ya_version_data.h>

#include <util/stream/str.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TString CreateYtVersion(int major, int minor, int patch, const TStringBuf& branch) {
    TStringStream out;
    out << major << "." << minor << "." << patch;
    out << "-" << branch;
    out << "-ya~" << TString(ARCADIA_SOURCE_REVISION).substr(0, 7);

    TString buildUser = BUILD_USER;
    if (buildUser != "teamcity") {
        out << "+" << buildUser;
    }

    return out.Str();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
