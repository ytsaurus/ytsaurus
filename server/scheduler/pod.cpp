#include "pod.h"
#include "pod_set.h"

namespace NYP::NServer::NScheduler {

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TPod::TPod(
    const TObjectId& id,
    TPodSet* podSet,
    NServer::NObjects::NProto::TMetaEtc metaEtc,
    TNode* node,
    NServer::NObjects::NProto::TPodSpecEtc specEtc,
    TAccount* account,
    NServer::NObjects::NProto::TPodStatusEtc statusEtc,
    TYsonString labels)
    : TObject(id, std::move(labels))
    , PodSet_(podSet)
    , MetaEtc_(std::move(metaEtc))
    , Node_(node)
    , SpecEtc_(std::move(specEtc))
    , Account_(account)
    , StatusEtc_(std::move(statusEtc))
{ }

TAccount* TPod::GetEffectiveAccount() const
{
    return Account_ ? Account_ : PodSet_->GetAccount();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NScheduler::NObjects
