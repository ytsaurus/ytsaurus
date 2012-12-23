#pragma once

#include <ytlib/ytree/ypath_proxy.h>

#include <ytlib/account_client/account_ypath.pb.h>

namespace NYT {
namespace NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TAccountYPathProxy
    : public NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Remove);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityClient
} // namespace NYT
