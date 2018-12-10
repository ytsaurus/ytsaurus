#pragma once

#include <yt/ytlib/account_client/account_ypath.pb.h>

#include <yt/core/ytree/ypath_proxy.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TAccountYPathProxy
    : public NYTree::TYPathProxy
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
