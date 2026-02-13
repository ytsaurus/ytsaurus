#pragma once

#include "cross_cluster_client.h"

namespace NYT::NCrossClusterReplicatedState {

////////////////////////////////////////////////////////////////////////////////

class ICrossClusterReplicatedState
    : public TRefCounted
{
public:
    virtual TFuture<void> ValidateStateDirectories() = 0;
    virtual TFuture<THashMap<TString, TString>> FetchVersions() = 0;

    // NB: Calling this concurrently with the same tag will result in a data race. Calling with distinct tags is safe.
    virtual ICrossClusterReplicatedValuePtr Value(std::string tag, NYPath::TYPath path) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICrossClusterReplicatedState);

ICrossClusterReplicatedStatePtr CreateCrossClusterReplicatedState(
    IMultiClusterClientPtr client,
    ICrossClusterReplicaLockWaiterPtr lockWaiter,
    TCrossClusterReplicatedStateConfigPtr config,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrossClusterReplicatedState
