#pragma once

#include <yt/ytlib/security_client/proto/account_ypath.pb.h>

#include <yt/core/ytree/ypath_proxy.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TAccountYPathProxy
    : public NYTree::TYPathProxy
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
