#include "pod_set.h"

namespace NYP {
namespace NServer {
namespace NScheduler {

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TPodSet::TPodSet(
    const TObjectId& id,
    TYsonString labels,
    TNodeSegment* nodeSegment,
    std::vector<NClient::NApi::NProto::TPodSetSpec_TAntiaffinityConstraint> antiaffinityConstraints)
    : TObject(id, std::move(labels))
    , NodeSegment_(nodeSegment)
    , AntiaffinityConstraints_(std::move(antiaffinityConstraints))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NScheduler
} // namespace NYP

