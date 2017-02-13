#include "helpers.h"

#include <yt/core/ypath/token.h>

namespace NYT {
namespace NSecurityClient {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TYPath GetUserPath(const Stroka& name)
{
    return "//sys/users/" + ToYPathLiteral(name);
}

TYPath GetGroupPath(const Stroka& name)
{
    return "//sys/groups/" + ToYPathLiteral(name);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityClient
} // namespace NYT

