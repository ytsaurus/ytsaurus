#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <core/rpc/config.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TPeerConnectionConfig
    : public NRpc::TRetryingChannelConfig
    , public NRpc::TBalancingChannelConfig
{
public:
    //! Id of the cell.
    TCellGuid CellGuid;

    TPeerConnectionConfig()
    {
        RegisterParameter("cell_guid", CellGuid)
            .Default();
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
            .Default(3);
        RegisterParameter("changelog_read_quorum", ChangelogReadQuorum)
            .GreaterThan(0)
            .Default(2);
        RegisterParameter("changelog_write_quorum", ChangelogWriteQuorum)
            .GreaterThan(0)
            .Default(2);
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteChangelogStoreOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
