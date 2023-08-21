#include "config.h"

#include <yt/yt/ytlib/journal_client/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NHydra {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TPeerConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cell_id", &TThis::CellId)
        .Default();
    registrar.Parameter("ignore_peer_state", &TThis::IgnorePeerState)
        .Default(false);

    registrar.Preprocessor([] (TThis* config) {
        // Query all peers in parallel.
        config->MaxConcurrentDiscoverRequests = std::numeric_limits<int>::max();
    });

    registrar.Postprocessor([] (TThis* config) {
        if (!config->CellId) {
            THROW_ERROR_EXCEPTION("\"cell_id\" cannot be equal to %v",
                NullCellId);
        }
    });
}

void TRemoteSnapshotStoreOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("snapshot_replication_factor", &TThis::SnapshotReplicationFactor)
        .GreaterThan(0)
        .InRange(1, NChunkClient::MaxReplicationFactor)
        .Default(3);
    registrar.Parameter("snapshot_compression_codec", &TThis::SnapshotCompressionCodec)
        .Default(NCompression::ECodec::Lz4);
    registrar.Parameter("snapshot_account", &TThis::SnapshotAccount)
        .NonEmpty();
    registrar.Parameter("snapshot_primary_medium", &TThis::SnapshotPrimaryMedium)
        .Default(NChunkClient::DefaultStoreMediumName);
    registrar.Parameter("snapshot_erasure_codec", &TThis::SnapshotErasureCodec)
        .Default(NErasure::ECodec::None);
    registrar.Parameter("snapshot_enable_striped_erasure", &TThis::SnapshotEnableStripedErasure)
        .Default(false);
    registrar.Parameter("snapshot_acl", &TThis::SnapshotAcl)
        .Default(BuildYsonNodeFluently()
            .BeginList()
            .EndList()->AsList());
}

void TRemoteChangelogStoreOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("changelog_erasure_codec", &TThis::ChangelogErasureCodec)
        .Default(NErasure::ECodec::None);
    registrar.Parameter("changelog_replication_factor", &TThis::ChangelogReplicationFactor)
        .GreaterThan(0)
        .InRange(1, NChunkClient::MaxReplicationFactor)
        .Default(3);
    registrar.Parameter("changelog_read_quorum", &TThis::ChangelogReadQuorum)
        .GreaterThan(0)
        .InRange(1, NChunkClient::MaxReplicationFactor)
        .Default(2);
    registrar.Parameter("changelog_write_quorum", &TThis::ChangelogWriteQuorum)
        .GreaterThan(0)
        .InRange(1, NChunkClient::MaxReplicationFactor)
        .Default(2);
    registrar.Parameter("enable_changelog_multiplexing", &TThis::EnableChangelogMultiplexing)
        .Default(true);
    registrar.Parameter("enable_changelog_chunk_preallocation", &TThis::EnableChangelogChunkPreallocation)
        .Default(false);
    registrar.Parameter("changelog_replica_lag_limit", &TThis::ChangelogReplicaLagLimit)
        .Default(NJournalClient::DefaultReplicaLagLimit);
    registrar.Parameter("changelog_external_cell_tag", &TThis::ChangelogExternalCellTag)
        .Optional();
    registrar.Parameter("changelog_account", &TThis::ChangelogAccount)
        .NonEmpty();
    registrar.Parameter("changelog_primary_medium", &TThis::ChangelogPrimaryMedium)
        .Default(NChunkClient::DefaultStoreMediumName);
    registrar.Parameter("changelog_acl", &TThis::ChangelogAcl)
        .Default(BuildYsonNodeFluently()
            .BeginList()
            .EndList()->AsList());

    registrar.Postprocessor([] (TThis* config) {
        NJournalClient::ValidateJournalAttributes(
            config->ChangelogErasureCodec,
            config->ChangelogReplicationFactor,
            config->ChangelogReadQuorum,
            config->ChangelogWriteQuorum);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
