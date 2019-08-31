#include "pod_disruption_budget.h"

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

TPodDisruptionBudget::TPodDisruptionBudget(
    TObjectId id,
    NYT::NYson::TYsonString labels,
    TObjectId uuid,
    NClient::NApi::NProto::TPodDisruptionBudgetSpec spec)
    : TObject(std::move(id), std::move(labels))
    , Uuid_(std::move(uuid))
    , Spec_(std::move(spec))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
