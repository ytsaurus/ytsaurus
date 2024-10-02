#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NOrm::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

class TRequestTrackerConfig
    : public NYT::NYTree::TYsonStruct
{
public:
    bool Enabled;
    i64 DefaultUserRequestWeightRateLimit;
    int DefaultUserRequestQueueSizeLimit;
    size_t ReconfigureBatchSize;

    int QueueSizeOverdraftMultiplier;
    i64 MaxThrottlerDebtMultiplier;

    REGISTER_YSON_STRUCT(TRequestTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRequestTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

class TAccessControlManagerConfig
    : public NYT::NYTree::TYsonStruct
{
public:
    TDuration ClusterStateUpdatePeriod;
    TRequestTrackerConfigPtr RequestTracker;
    // Superusers do not require acl loading, but for the sake of performance statistics fairness in tests,
    // acl may still be loaded via this option.
    bool PreloadAclForSuperusers;
    bool EnableObjectTableReader;
    NObjects::TObjectTableReaderConfigPtr ObjectTableReader;

    std::vector<NObjects::TObjectTypeValue> GetClusterStateAllowedObjectTypes() const;

    REGISTER_YSON_STRUCT(TAccessControlManagerConfig);

    static void Register(TRegistrar registrar);

protected:
    static void OverrideClusterStateAllowedObjectTypesDefault(
        TRegistrar registrar,
        std::vector<NObjects::TObjectTypeName> clusterStateAllowedObjectTypes);

private:
    std::vector<NObjects::TObjectTypeName> ClusterStateAllowedObjectTypeNames_;
};

DEFINE_REFCOUNTED_TYPE(TAccessControlManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl
