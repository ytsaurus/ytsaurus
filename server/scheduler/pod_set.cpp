#include "pod_set.h"

namespace NYP::NServer::NScheduler {

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TPodSet::TPodSet(
    TObjectId id,
    TYsonString labels,
    TObjectId nodeSegmentId,
    TObjectId accountId,
    TObjectId podDisruptionBudgetId,
    std::vector<NClient::NApi::NProto::TAntiaffinityConstraint> antiaffinityConstraints,
    TString nodeFilter)
    : TObject(std::move(id), std::move(labels))
    , NodeSegmentId_(std::move(nodeSegmentId))
    , AccountId_(std::move(accountId))
    , PodDisruptionBudgetId_(std::move(podDisruptionBudgetId))
    , AntiaffinityConstraints_(std::move(antiaffinityConstraints))
    , NodeFilter_(std::move(nodeFilter))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
