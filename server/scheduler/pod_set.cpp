#include "pod_set.h"

namespace NYP::NServer::NScheduler {

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TPodSet::TPodSet(
    const TObjectId& id,
    TYsonString labels,
    TNodeSegment* nodeSegment,
    TAccount* account,
    std::vector<NClient::NApi::NProto::TAntiaffinityConstraint> antiaffinityConstraints,
    TString nodeFilter)
    : TObject(id, std::move(labels))
    , NodeSegment_(nodeSegment)
    , Account_(account)
    , AntiaffinityConstraints_(std::move(antiaffinityConstraints))
    , NodeFilter_(std::move(nodeFilter))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
