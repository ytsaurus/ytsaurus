#include "config.h"

#include <yt/yt/client/security_client/public.h>

namespace NYT::NCrossClusterReplicatedState {

////////////////////////////////////////////////////////////////////////////////

void TCrossClusterStateReplicaConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster_name", &TThis::ClusterName)
        .Default();
    registrar.Parameter("state_directory", &TThis::StateDirectory)
        .Default();

    registrar.Postprocessor([] (const TThis* config) {
        THROW_ERROR_EXCEPTION_IF(config->StateDirectory.empty(),
            "\"state_directory\" cannot be empty");
        THROW_ERROR_EXCEPTION_IF(config->StateDirectory.EndsWith('/'),
            "\"state_directory\" cannot be root or end with slash");
    });
}

////////////////////////////////////////////////////////////////////////////////

void TCrossClusterReplicatedStateConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("request_timeout", &TThis::RequestTimeout)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("lock_waiting_timeout", &TThis::LockWaitingTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("update_transaction_timeout", &TThis::UpdateTransactionTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("lock_check_period", &TThis::LockCheckPeriod)
        .Default(TDuration::MilliSeconds(200));
    registrar.Parameter("user", &TThis::User)
        .Default(NSecurityClient::RootUserName);
    registrar.Parameter("replicas", &TThis::Replicas)
        .Default();

    registrar.Postprocessor([] (const TThis* config) {
        THROW_ERROR_EXCEPTION_IF(config->Replicas.size() < 3,
            "State must have at least 3 replicas");

        THashSet<std::string> uniqueClusterNames;
        for (const auto& replica : config->Replicas) {
            uniqueClusterNames.insert(replica->ClusterName);
        }
        THROW_ERROR_EXCEPTION_IF(uniqueClusterNames.size() != config->Replicas.size(),
            "Replica clusters must be unique");
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrossClusterReplicatedState
