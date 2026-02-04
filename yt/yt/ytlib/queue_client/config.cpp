#include "config.h"

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NQueueClient {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

bool TLookupSessionConfig::operator==(const TLookupSessionConfig& other) const
{
    return std::tie(User, Table) == std::tie(other.User, other.Table);
}

////////////////////////////////////////////////////////////////////////////////

TStateLookupCacheConfig& TStateLookupCacheConfig::operator=(const TStateLookupCacheConfigPtr& other)
{
    // TODO(apachee): Add batch lookup config.
    Cache = other->Cache;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

bool TStateLookupCacheConfig::operator==(const TStateLookupCacheConfig& other) const
{
    // TODO(apachee): Add batch lookup config.
    return *Cache == *other.Cache;
}

////////////////////////////////////////////////////////////////////////////////

TStateLookupCacheConfigPtr TStateLookupCacheConfig::FromQueueConsumerRegistrationManagerCacheConfig(
    const TQueueConsumerRegistrationManagerCacheConfigPtr& config,
    EQueueConsumerRegistrationManagerCacheKind cacheKind)
{
    auto result = New<NDetail::TStateLookupCacheConfig>();
    // NB(apachee): #TAsyncExpiringCacheConfig::ApplyDynamic copies #Base internally.
    result->Cache = config->Base->ApplyDynamic(config->Delta[cacheKind]);
    // TODO(apachee): Add batch lookup config.
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool TCompoundStateLookupCacheConfig::operator==(const TCompoundStateLookupCacheConfig& other) const
{
    return
        std::tie(static_cast<const TLookupSessionConfig&>(*this), static_cast<const TStateLookupCacheConfig&>(*this)) ==
        std::tie(static_cast<const TLookupSessionConfig&>(other), static_cast<const TStateLookupCacheConfig&>(other));
}

////////////////////////////////////////////////////////////////////////////////

TCompoundStateLookupCacheConfigPtr TCompoundStateLookupCacheConfig::FromQueueConsumerRegistrationManagerConfig(
    const TQueueConsumerRegistrationManagerConfigPtr& config,
    EQueueConsumerRegistrationManagerCacheKind cacheKind)
{
    YT_VERIFY(config->Implementation == EQueueConsumerRegistrationManagerImplementation::AsyncExpiringCache);

    auto result = New<NDetail::TCompoundStateLookupCacheConfig>();

    // Initialize state lookup cache config.

    auto stateLookupCacheConfig = TStateLookupCacheConfig::FromQueueConsumerRegistrationManagerCacheConfig(config->Cache, cacheKind);
    static_cast<NDetail::TStateLookupCacheConfig&>(*result) = stateLookupCacheConfig;

    // Initialize lookup session config.

    result->User = config->User;
    switch (cacheKind) {
        case EQueueConsumerRegistrationManagerCacheKind::ListRegistrations:
            [[fallthrough]];
        case EQueueConsumerRegistrationManagerCacheKind::RegistrationLookup:
            result->Table = config->StateReadPath;
            break;
        case EQueueConsumerRegistrationManagerCacheKind::ReplicaMappingLookup:
            result->Table = config->ReplicaMappingReadPath;
            break;
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

void TQueueAgentStageChannelConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("default_request_timeout", &TThis::DefaultRequestTimeout)
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

void TQueueAgentDynamicStateConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("root", &TThis::Root)
        .Default("//sys/queue_agents");
    registrar.Parameter("consumer_registration_table_path", &TThis::ConsumerRegistrationTablePath)
        .Default("//sys/queue_agents/consumer_registrations");
    registrar.Parameter("replicated_table_mapping_table_path", &TThis::ReplicatedTableMappingTablePath)
        .Default("//sys/queue_agents/replicated_table_mapping");
}

////////////////////////////////////////////////////////////////////////////////

void TQueueConsumerRegistrationManagerCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("base", &TThis::Base)
        .DefaultNew();
    registrar.Parameter("delta", &TThis::Delta)
        .DefaultCtor([] {
            decltype(TThis::Delta) delta;

            for (const auto& cacheKind : TEnumTraits<EQueueConsumerRegistrationManagerCacheKind>::GetDomainValues()) {
                delta[cacheKind] = New<TAsyncExpiringCacheDynamicConfig>();
            }

            return delta;
        });

    registrar.Preprocessor([] (TThis* config) {
        // NB(apachee): Batching lookups and selects to dynamic state is a must.
        config->Base->BatchUpdate = true;

        // TODO(apachee): Provide defaults for registration cache.
    });
}

////////////////////////////////////////////////////////////////////////////////

void TQueueConsumerRegistrationManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("state_write_path", &TThis::StateWritePath)
        .Default("//sys/queue_agents/consumer_registrations");
    registrar.Parameter("state_read_path", &TThis::StateReadPath)
        .Default("//sys/queue_agents/consumer_registrations");
    registrar.Parameter("replicated_table_mapping_read_path", &TThis::ReplicatedTableMappingReadPath)
        .Default("//sys/queue_agents/replicated_table_mapping");
    registrar.Parameter("replica_mapping_read_path", &TThis::ReplicaMappingReadPath)
        .Default("//sys/queue_agents/replica_mapping");
    registrar.Parameter("bypass_caching", &TThis::BypassCaching)
        .Default(false);
    registrar.Parameter("configuration_refresh_period", &TThis::ConfigurationRefreshPeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("user", &TThis::User)
        .Default(RootUserName);
    registrar.Parameter("resolve_symlinks", &TThis::ResolveSymlinks)
        .Default(false);
    registrar.Parameter("resolve_replicas", &TThis::ResolveReplicas)
        .Default(false);
    registrar.Parameter("disable_list_all_registrations", &TThis::DisableListAllRegistrations)
        .Default(false);
    registrar.Parameter("implementation", &TThis::Implementation)
        .Default(EQueueConsumerRegistrationManagerImplementation::Legacy);
    registrar.Parameter("cache_refresh_period", &TThis::CacheRefreshPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("cache", &TThis::Cache)
        .DefaultNew();

    registrar.Postprocessor([] (TThis* config) {
        if (config->Implementation == EQueueConsumerRegistrationManagerImplementation::AsyncExpiringCache && !config->DisableListAllRegistrations) {
            THROW_ERROR_EXCEPTION(
                "%v implementation requires option \"disable_list_all_registrations\" to be true",
                config->Implementation);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TQueueAgentConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("stages", &TThis::Stages)
        .Default();
    registrar.Parameter("queue_consumer_registration_manager", &TThis::QueueConsumerRegistrationManager)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TQueueProducerSystemMeta::Register(TRegistrar registrar)
{
    registrar.Parameter("mutation_id", &TThis::MutationId)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
