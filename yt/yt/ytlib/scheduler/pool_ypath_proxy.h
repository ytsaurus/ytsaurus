#pragma once

#include <yt/yt/ytlib/scheduler/proto/pool_ypath.pb.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerPoolYPathProxy
    : public NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY(TSchedulerPool);

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, TransferPoolResources);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
