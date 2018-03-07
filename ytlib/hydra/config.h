#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/compression/public.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TPeerConnectionConfig
    : public NRpc::TBalancingChannelConfig
{
public:
    TCellId CellId;

    TPeerConnectionConfig()
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
};

DEFINE_REFCOUNTED_TYPE(TPeerConnectionConfig)

class TRemoteSnapshotStoreOptions
    : public virtual NYTree::TYsonSerializable
{
public:
    int SnapshotReplicationFactor;
    NCompression::ECodec SnapshotCompressionCodec;
    TString SnapshotAccount;
    TString SnapshotPrimaryMedium;

    TRemoteSnapshotStoreOptions()
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
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteSnapshotStoreOptions)

class TRemoteChangelogStoreOptions
    : public virtual NYTree::TYsonSerializable
{
public:
    int ChangelogReplicationFactor;
    int ChangelogReadQuorum;
    int ChangelogWriteQuorum;
    bool EnableChangelogMultiplexing;
    TString ChangelogAccount;
    TString ChangelogPrimaryMedium;

    TRemoteChangelogStoreOptions()
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

        RegisterPostprocessor([&] () {
            if (ChangelogReadQuorum + ChangelogWriteQuorum < ChangelogReplicationFactor + 1) {
                THROW_ERROR_EXCEPTION("Read/write quorums are not safe: changelog_read_quorum + changelog_write_quorum < changelog_replication_factor + 1");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteChangelogStoreOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
