#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NCrossClusterReplicatedState {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((InvalidNodeFormat) (21000))
);

DECLARE_REFCOUNTED_CLASS(ICrossClusterCallbackExecutor);
DECLARE_REFCOUNTED_CLASS(ISingleClusterClient);
DECLARE_REFCOUNTED_CLASS(IMultiClusterClient);

DECLARE_REFCOUNTED_CLASS(ICrossClusterReplicaLockWaiter);
DECLARE_REFCOUNTED_CLASS(ICrossClusterReplicatedState);
DECLARE_REFCOUNTED_CLASS(ICrossClusterReplicatedValue);

DECLARE_REFCOUNTED_STRUCT(TCrossClusterReplicatedStateConfig);
DECLARE_REFCOUNTED_STRUCT(TCrossClusterStateReplicaConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrossClusterReplicatedState
