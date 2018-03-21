#include "pod.h"

namespace NYP {
namespace NServer {
namespace NScheduler {

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TPod::TPod(
    const TObjectId& id,
    TPodSet* podSet,
    TYsonString labels,
    TNode* node,
    NServer::NObjects::NProto::TPodSpecOther specOther,
    NServer::NObjects::NProto::TPodStatusOther statusOther)
    : TObject(id, std::move(labels))
    , PodSet_(podSet)
    , Node_(node)
    , SpecOther_(std::move(specOther))
    , StatusOther_(std::move(statusOther))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NScheduler
} // namespace NYP

