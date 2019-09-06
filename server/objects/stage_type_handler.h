#pragma once

#include "public.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IObjectTypeHandler> CreateStageTypeHandler(NMaster::TBootstrap* bootstrap);
void ValidateTvmConfig(const NClient::NApi::NProto::TTvmConfig& config);
void ValidateStageAndDeployUnitId(const TObjectId& id, const TString& description);
void ValidatePodAgentWorkloadEnv(const NInfra::NPodAgent::API::TWorkload& workload);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
