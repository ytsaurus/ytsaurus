#pragma once

#include <yt/yt/ytlib/object_client/proto/master_ypath.pb.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

struct TMasterYPathProxy
{
    DEFINE_YPATH_PROXY(Master);

    // NB: when introducing a new method here, consider marking up such requests
    // with suppress_transaction_coordinator_sync.

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, CreateObject);
    DEFINE_YPATH_PROXY_METHOD(NProto, GetClusterMeta);
    DEFINE_YPATH_PROXY_METHOD(NProto, CheckPermissionByAcl);

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, AddMaintenance);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, RemoveMaintenance);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
