#include "access_control_hierarchy.h"

namespace NYT::NOrm::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

TSnapshotAccessControlHierarchy::TSnapshotAccessControlHierarchy(
    NObjects::TObjectManagerPtr objectManager,
    TClusterObjectSnapshotPtr clusterObjectSnapshot)
    : ObjectManager_(std::move(objectManager))
    , ClusterObjectSnapshot_(std::move(clusterObjectSnapshot))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl
