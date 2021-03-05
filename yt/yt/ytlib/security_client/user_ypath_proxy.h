#pragma once

#include <yt/yt/ytlib/account_client/user_ypath.pb.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TUserYPathProxy
    : public NYTree::TYPathProxy
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
