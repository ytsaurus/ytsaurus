#include "pod.h"
#include "pod_set.h"

namespace NYP::NServer::NScheduler {

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TPod::TPod(
    const TObjectId& id,
    TPodSet* podSet,
    NServer::NObjects::NProto::TMetaOther metaOther,
    TNode* node,
    NServer::NObjects::NProto::TPodSpecOther specOther,
    TAccount* account,
    NServer::NObjects::NProto::TPodStatusOther statusOther,
    TYsonString labels)
    : TObject(id, std::move(labels))
    , PodSet_(podSet)
    , MetaOther_(std::move(metaOther))
    , Node_(node)
    , SpecOther_(std::move(specOther))
    , Account_(account)
    , StatusOther_(std::move(statusOther))
{ }

TAccount* TPod::GetEffectiveAccount() const
{
    return Account_ ? Account_ : PodSet_->GetAccount();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NScheduler::NObjects
