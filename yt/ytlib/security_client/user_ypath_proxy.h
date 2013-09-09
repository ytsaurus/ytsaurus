#pragma once

#include <core/ytree/ypath_proxy.h>

#include <ytlib/account_client/user_ypath.pb.h>

namespace NYT {
namespace NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TUserYPathProxy
    : public NYTree::TYPathProxy
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityClient
} // namespace NYT
