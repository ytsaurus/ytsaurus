#include "helpers.h"

#include <yt/core/ypath/token.h>

namespace NYT::NSecurityClient {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TYPath GetUserPath(const TString& name)
{
    return "//sys/users/" + ToYPathLiteral(name);
}

TYPath GetGroupPath(const TString& name)
{
    return "//sys/groups/" + ToYPathLiteral(name);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient

