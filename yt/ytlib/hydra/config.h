#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <core/rpc/config.h>

#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TPeerConnectionConfig
    : public NRpc::TRetryingChannelConfig
    , public NRpc::TBalancingChannelConfig
{
public:
    //! Id of the cell.
    TCellId CellId;

    TPeerConnectionConfig()
    {
        RegisterParameter("cell_guid", CellId)
            .Default();

        RegisterInitializer([&] () {
            // Query all peers in parallel.
            MaxConcurrentDiscoverRequests = std::numeric_limits<int>::max();
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TPeerConnectionConfig)

class TRemoteSnapshotStoreOptions
    : public virtual NYTree::TYsonSerializable
{
public:
    int SnapshotReplicationFactor;

    TRemoteSnapshotStoreOptions()
    {
        RegisterParameter("snapshot_replication_factor", SnapshotReplicationFactor)
            .GreaterThan(0)
            .InRange(1, NChunkClient::MaxReplicationFactor)
            .Default(3);
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

        RegisterValidator([&] () {
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
