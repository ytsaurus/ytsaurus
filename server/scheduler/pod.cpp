#include "pod.h"

namespace NYP {
namespace NServer {
namespace NScheduler {

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TPod::TPod(
    const TObjectId& id,
    TPodSet* podSet,
    NServer::NObjects::NProto::TMetaOther metaOther,
    TNode* node,
    NServer::NObjects::NProto::TPodSpecOther specOther,
    NServer::NObjects::NProto::TPodStatusOther statusOther,
    TYsonString labels)
    : TObject(id, std::move(labels))
    , PodSet_(podSet)
    , MetaOther_(std::move(metaOther))
    , Node_(node)
    , SpecOther_(std::move(specOther))
    , StatusOther_(std::move(statusOther))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NScheduler
} // namespace NYP
