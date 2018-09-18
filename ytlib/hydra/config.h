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

    TPeerConnectionConfig();
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
    NYTree::IListNodePtr SnapshotAcl;

    TRemoteSnapshotStoreOptions();
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
    NYTree::IListNodePtr ChangelogAcl;

    TRemoteChangelogStoreOptions();
};

DEFINE_REFCOUNTED_TYPE(TRemoteChangelogStoreOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
