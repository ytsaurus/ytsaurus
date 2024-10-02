#include "config.h"

#include <yt/yt/orm/client/objects/registry.h>
#include <yt/yt/orm/client/objects/type.h>

#include <yt/yt/orm/server/objects/config.h>

#include <yt/yt/core/concurrency/config.h>

namespace NYT::NOrm::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

void TRequestTrackerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(true);
    registrar.Parameter("default_user_request_weight_rate_limit", &TThis::DefaultUserRequestWeightRateLimit)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(100'000)
        .Default(100);
    registrar.Parameter("default_user_request_queue_size_limit", &TThis::DefaultUserRequestQueueSizeLimit)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(100'000)
        .Default(100);
    registrar.Parameter("reconfigure_batch_size", &TThis::ReconfigureBatchSize)
        .GreaterThan(0)
        .LessThan(100'000)
        .Default(500);
    registrar.Parameter(
        "queue_size_overdraft_multiplier",
        &TThis::QueueSizeOverdraftMultiplier)
        .GreaterThanOrEqual(1)
        .LessThan(10)
        .Default(2);
    registrar.Parameter(
        "max_throttler_debt_multiplier",
        &TThis::MaxThrottlerDebtMultiplier)
        .GreaterThanOrEqual(1)
        .LessThan(10)
        .Default(5);
}

////////////////////////////////////////////////////////////////////////////////

std::vector<NObjects::TObjectTypeValue> TAccessControlManagerConfig::GetClusterStateAllowedObjectTypes() const
{
    std::vector<NObjects::TObjectTypeValue> result;
    for (const auto& typeName : ClusterStateAllowedObjectTypeNames_) {
        result.push_back(NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeValueByNameOrCrash(typeName));
    }
    return result;
}

void TAccessControlManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster_state_update_period", &TThis::ClusterStateUpdatePeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("request_tracker", &TThis::RequestTracker)
        .DefaultNew();
    registrar.Parameter("cluster_state_allowed_object_types", &TThis::ClusterStateAllowedObjectTypeNames_)
        .Default();
    registrar.Parameter("preload_acl_for_superusers", &TThis::PreloadAclForSuperusers)
        .Default(false);
    registrar.Parameter("enable_object_table_reader", &TThis::EnableObjectTableReader)
        .Default(true);
    registrar.Parameter("object_table_reader", &TThis::ObjectTableReader)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        config->ClusterStateAllowedObjectTypeNames_.push_back(NClient::NObjects::TObjectTypeNames::Schema);
    });

    registrar.Postprocessor([] (TThis* config) {
        for (const auto& typeName : config->ClusterStateAllowedObjectTypeNames_) {
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeByNameOrThrow(typeName);
        }
    });
}

void TAccessControlManagerConfig::OverrideClusterStateAllowedObjectTypesDefault(
    TRegistrar registrar,
    std::vector<NObjects::TObjectTypeName> clusterStateAllowedObjectTypes)
{
    registrar.Preprocessor([clusterStateAllowedObjectTypes = std::move(clusterStateAllowedObjectTypes)] (TThis* config) {
        config->ClusterStateAllowedObjectTypeNames_ = std::move(clusterStateAllowedObjectTypes);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl
