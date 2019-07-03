#include "pod_disruption_budget.h"

namespace NYP::NServer::NScheduler {

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TPodDisruptionBudget::TPodDisruptionBudget(
    const TObjectId& id,
    TYsonString labels,
    NServer::NObjects::NProto::TMetaEtc metaEtc,
    NClient::NApi::NProto::TPodDisruptionBudgetSpec spec)
    : TObject(id, std::move(labels))
    , MetaEtc_(std::move(metaEtc))
    , Spec_(std::move(spec))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
