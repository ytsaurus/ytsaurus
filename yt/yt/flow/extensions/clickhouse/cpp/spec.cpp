#include "spec.h"

#include "shard.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TCommonClickHouseSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("host", &TThis::Host)
        .Default();
    registrar.Parameter("port", &TThis::Port)
        .Default(9000);
    registrar.Parameter("hosts", &TThis::Hosts)
        .Default();
    registrar.Parameter("host_selection_policy", &TThis::HostSelectionPolicy)
        .Default(EClickHouseHostSelectionPolicy::OrderedRoundRobin);

    registrar.Parameter("shard_hosts", &TThis::ShardHosts)
        .Default();
    registrar.Parameter("sharding_key_columns", &TThis::ShardingKeyColumns)
        .Default();

    registrar.Parameter("user", &TThis::User)
        .Default("default");
    registrar.Parameter("password_env_var", &TThis::PasswordEnvVar)
        .Default();

    registrar.Parameter("database", &TThis::Database)
        .Default("default");
    registrar.Parameter("table", &TThis::Table);

    registrar.Parameter("codec", &TThis::Codec)
        .Default(EClickHouseCodec::Lz4);

    registrar.Parameter("enable_tls", &TThis::EnableTls)
        .Default(false);
    registrar.Parameter("tls_ca_files", &TThis::TlsCaFiles)
        .Default();
    registrar.Parameter("tls_ca_directory", &TThis::TlsCaDirectory)
        .Default();
    registrar.Parameter("tls_skip_verification", &TThis::TlsSkipVerification)
        .Default(false);

    registrar.Postprocessor([] (TThis* parameters) {
        THROW_ERROR_EXCEPTION_IF(
            !parameters->EnableTls &&
                (!parameters->TlsCaFiles.empty() ||
                    !parameters->TlsCaDirectory.empty() ||
                    parameters->TlsSkipVerification),
            "tls_* parameters require enable_tls = true");

        int setHostForms = !parameters->Host.empty() +
            !parameters->Hosts.empty() +
            !parameters->ShardHosts.empty();
        if (setHostForms == 0) {
            THROW_ERROR_EXCEPTION("One of \"host\", \"hosts\" or \"shard_hosts\" must be set");
        }
        if (setHostForms > 1) {
            THROW_ERROR_EXCEPTION(
                "\"host\", \"hosts\" and \"shard_hosts\" are mutually exclusive");
        }

        if (parameters->ShardHosts.empty()) {
            if (!parameters->ShardingKeyColumns.empty()) {
                THROW_ERROR_EXCEPTION(
                    "\"sharding_key_columns\" requires \"shard_hosts\"; the unsharded forms "
                    "route every row to the same shard");
            }
            for (const auto& host : parameters->Hosts) {
                if (host.empty()) {
                    THROW_ERROR_EXCEPTION("\"hosts\" contains an empty host");
                }
            }
            if (parameters->Hosts.size() == 1) {
                THROW_ERROR_EXCEPTION(
                    "\"hosts\" with a single entry is just \"host\"; use \"host\" instead");
            }
            return;
        }
        for (const auto& [name, hosts] : parameters->ShardHosts) {
            if (!IsValidShardName(name)) {
                THROW_ERROR_EXCEPTION(
                    "Shard name %Qv must be a non-empty string of "
                    "letters, digits, underscores and hyphens",
                    name);
            }
            if (hosts.empty()) {
                THROW_ERROR_EXCEPTION("Shard %Qv has no hosts", name);
            }
            for (const auto& host : hosts) {
                if (host.empty()) {
                    THROW_ERROR_EXCEPTION("Shard %Qv has an empty host", name);
                }
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TClickHouseShardTopologyState::Register(TRegistrar registrar)
{
    registrar.Parameter("topology_fingerprint", &TThis::TopologyFingerprint)
        .Default();
    registrar.Parameter("target_identity_fingerprints", &TThis::TargetIdentityFingerprints)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicCommonClickHouseSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("write_timeout", &TThis::WriteTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("retry_backoff", &TThis::RetryBackoff)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("max_insert_attempts", &TThis::MaxInsertAttempts)
        .GreaterThanOrEqual(1)
        .Default(10);

    registrar.Parameter("async_insert", &TThis::AsyncInsert)
        .Default(false);

    registrar.Parameter("replay_horizon", &TThis::ReplayHorizon)
        .Default(TDuration::Days(1));
}

////////////////////////////////////////////////////////////////////////////////

void TClickHouseBatchingSinkBaseParameters::Register(TRegistrar /*registrar*/)
{ }

void TClickHouseBatchingSinkParameters::Register(TRegistrar registrar)
{
    registrar.Postprocessor([] (TThis* parameters) {
        THROW_ERROR_EXCEPTION_IF(!parameters->ShardHosts.empty(),
            "TClickHouseBatchingSink rejects shard_hosts; use TShardedClickHouseBatchingSink");
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicClickHouseBatchingSinkParameters::Register(TRegistrar /*registrar*/)
{ }

void TShardedClickHouseBatchingSinkParameters::Register(TRegistrar registrar)
{
    registrar.Postprocessor([] (TThis* parameters) {
        THROW_ERROR_EXCEPTION_IF(parameters->ShardHosts.empty(),
            "TShardedClickHouseBatchingSink requires shard_hosts");
    });
}

void TDynamicShardedClickHouseBatchingSinkParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TAtLeastOnceClickHouseSinkParameters::Register(TRegistrar /*registrar*/)
{ }

void TDynamicAtLeastOnceClickHouseSinkParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TAtMostOnceClickHouseSinkParameters::Register(TRegistrar /*registrar*/)
{ }

void TDynamicAtMostOnceClickHouseSinkParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TClickHouseSinkControllerParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TDynamicClickHouseSinkControllerParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
