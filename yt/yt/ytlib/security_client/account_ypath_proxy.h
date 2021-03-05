#pragma once

#include <yt/yt/ytlib/security_client/proto/account_ypath.pb.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TAccountYPathProxy
    : public NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY(Account);

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, TransferAccountResources);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
