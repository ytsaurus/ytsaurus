#include "pod_disruption_budget.h"

namespace NYP::NServer::NScheduler {

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TPodDisruptionBudget::TPodDisruptionBudget(
    const TObjectId& id,
    TYsonString labels,
    TObjectId uuid,
    NClient::NApi::NProto::TPodDisruptionBudgetSpec spec)
    : TObject(id, std::move(labels))
    , Uuid_(std::move(uuid))
    , Spec_(std::move(spec))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
