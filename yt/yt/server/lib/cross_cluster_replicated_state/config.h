#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NCrossClusterReplicatedState {

////////////////////////////////////////////////////////////////////////////////

struct TCrossClusterStateReplicaConfig
    : public NYTree::TYsonStruct
{
    std::string ClusterName;
    NYPath::TYPath StateDirectory;

    REGISTER_YSON_STRUCT(TCrossClusterStateReplicaConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCrossClusterStateReplicaConfig);

////////////////////////////////////////////////////////////////////////////////

struct TCrossClusterReplicatedStateConfig
    : public NYTree::TYsonStruct
{
    TDuration RequestTimeout;
    TDuration UpdateTransactionTimeout;
    TDuration LockWaitingTimeout;
    TDuration LockCheckPeriod;
    std::string User;
    std::vector<TCrossClusterStateReplicaConfigPtr> Replicas;

    REGISTER_YSON_STRUCT(TCrossClusterReplicatedStateConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCrossClusterReplicatedStateConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrossClusterReplicatedState
