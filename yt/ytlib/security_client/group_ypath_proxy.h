#pragma once

#include <core/ytree/ypath_proxy.h>

#include <ytlib/security_client/group_ypath.pb.h>

namespace NYT {
namespace NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TGroupYPathProxy
    : public NYTree::TYPathProxy
{
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, AddMember);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, RemoveMember);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityClient
} // namespace NYT
