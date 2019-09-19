#pragma once

#include "public.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IObjectTypeHandler> CreateStageTypeHandler(NMaster::TBootstrap* bootstrap);
void ValidateTvmConfig(const NClient::NApi::NProto::TTvmConfig& config);
void ValidateStageAndDeployUnitId(const TObjectId& id, const TString& description);
void ValidatePodAgentSpec(const NInfra::NPodAgent::API::TPodAgentSpec& spec);
void ValidatePodAgentObjectId(const TString& id, const TString& description);
void ValidatePodAgentObjectEnv(
    const google::protobuf::RepeatedPtrField<NInfra::NPodAgent::API::TEnvVar>& env,
    const TString& objectId,
    const TString& description);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
