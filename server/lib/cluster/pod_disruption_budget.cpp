#include "pod_disruption_budget.h"

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

TPodDisruptionBudget::TPodDisruptionBudget(
    TObjectId id,
    NYT::NYson::TYsonString labels,
    TObjectId uuid,
    NClient::NApi::NProto::TPodDisruptionBudgetSpec spec,
    NClient::NApi::NProto::TPodDisruptionBudgetStatus status)
    : TObject(std::move(id), std::move(labels))
    , Uuid_(std::move(uuid))
    , Spec_(std::move(spec))
    , Status_(std::move(status))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
