#pragma once

#include "cross_cluster_client.h"

namespace NYT::NCrossClusterReplicatedState {

////////////////////////////////////////////////////////////////////////////////

class ICrossClusterReplicatedValue
    : public TRefCounted
{
public:
    static constexpr std::string_view VersionAttributeKey = "cross_cluster_replica_version";

    virtual TFuture<NYTree::INodePtr> Load() = 0;
    virtual TFuture<void> Store(const NYTree::IMapNodePtr& value) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICrossClusterReplicatedValue);

ICrossClusterReplicatedValuePtr CreateCrossClusterReplicatedValue(
    IMultiClusterClientPtr client,
    ICrossClusterReplicaLockWaiterPtr lockWaiter,
    TCrossClusterReplicatedStateConfigPtr config,
    std::string tag,
    std::vector<NYPath::TYPath> paths,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrossClusterReplicatedState
