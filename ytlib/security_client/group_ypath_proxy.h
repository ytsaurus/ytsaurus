#pragma once

#include <yt/ytlib/security_client/group_ypath.pb.h>

#include <yt/core/ytree/ypath_proxy.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TGroupYPathProxy
    : public NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY(Group);

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, AddMember);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, RemoveMember);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
