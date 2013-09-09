#pragma once

#include <core/ytree/ypath_proxy.h>

#include <ytlib/account_client/account_ypath.pb.h>

namespace NYT {
namespace NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TAccountYPathProxy
    : public NYTree::TYPathProxy
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityClient
} // namespace NYT
