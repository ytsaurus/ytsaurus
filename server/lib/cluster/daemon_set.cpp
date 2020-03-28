#include "daemon_set.h"

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

TDaemonSet::TDaemonSet(
    TObjectId id,
    NYT::NYson::TYsonString labels,
    TObjectId podSetId,
    NClient::NApi::NProto::TDaemonSetSpec spec)
    : TObject(std::move(id), std::move(labels))
    , PodSetId_(std::move(podSetId))
    , Spec_(std::move(spec))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
