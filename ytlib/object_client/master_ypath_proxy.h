#pragma once

#include <yt/ytlib/object_client/master_ypath.pb.h>

#include <yt/core/ytree/ypath_proxy.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

struct TMasterYPathProxy
{
    DEFINE_YPATH_PROXY(Master);

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, CreateObject);
    DEFINE_YPATH_PROXY_METHOD(NProto, GetClusterMeta);
    DEFINE_YPATH_PROXY_METHOD(NProto, CheckPermissionByAcl);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
