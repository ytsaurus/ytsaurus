#include "config.h"

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NHydra {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TPeerConnectionConfig::TPeerConnectionConfig()
{
    RegisterParameter("cell_id", CellId)
        .Default();

    RegisterPreprocessor([&] () {
        // Query all peers in parallel.
        MaxConcurrentDiscoverRequests = std::numeric_limits<int>::max();
    });

    RegisterPostprocessor([&] () {
       if (!CellId) {
           THROW_ERROR_EXCEPTION("\"cell_id\" cannot be equal to %v",
               NullCellId);
       }
    });
}

TRemoteSnapshotStoreOptions::TRemoteSnapshotStoreOptions()
{
    RegisterParameter("snapshot_replication_factor", SnapshotReplicationFactor)
        .GreaterThan(0)
        .InRange(1, NChunkClient::MaxReplicationFactor)
        .Default(3);
    RegisterParameter("snapshot_compression_codec", SnapshotCompressionCodec)
        .Default(NCompression::ECodec::Lz4);
    RegisterParameter("snapshot_account", SnapshotAccount)
        .NonEmpty();
    RegisterParameter("snapshot_primary_medium", SnapshotPrimaryMedium)
        .Default(NChunkClient::DefaultStoreMediumName);
    RegisterParameter("snapshot_acl", SnapshotAcl)
        .Default(BuildYsonNodeFluently()
            .BeginList()
            .EndList()->AsList());
}

TRemoteChangelogStoreOptions::TRemoteChangelogStoreOptions()
{
    RegisterParameter("changelog_replication_factor", ChangelogReplicationFactor)
        .GreaterThan(0)
        .InRange(1, NChunkClient::MaxReplicationFactor)
        .Default(3);
    RegisterParameter("changelog_read_quorum", ChangelogReadQuorum)
        .GreaterThan(0)
        .InRange(1, NChunkClient::MaxReplicationFactor)
        .Default(2);
    RegisterParameter("changelog_write_quorum", ChangelogWriteQuorum)
        .GreaterThan(0)
        .InRange(1, NChunkClient::MaxReplicationFactor)
        .Default(2);
    RegisterParameter("enable_changelog_multiplexing", EnableChangelogMultiplexing)
        .Default(true);
    RegisterParameter("changelog_account", ChangelogAccount)
        .NonEmpty();
    RegisterParameter("changelog_primary_medium", ChangelogPrimaryMedium)
        .Default(NChunkClient::DefaultStoreMediumName);
    RegisterParameter("changelog_acl", ChangelogAcl)
        .Default(BuildYsonNodeFluently()
            .BeginList()
            .EndList()->AsList());

    RegisterPostprocessor([&] () {
        if (ChangelogReadQuorum + ChangelogWriteQuorum < ChangelogReplicationFactor + 1) {
            THROW_ERROR_EXCEPTION("Read/write quorums are not safe: changelog_read_quorum + changelog_write_quorum < changelog_replication_factor + 1");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
