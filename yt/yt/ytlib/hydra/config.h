#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/library/erasure/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TPeerConnectionConfig
    : public NRpc::TBalancingChannelConfig
{
public:
    TCellId CellId;

    TPeerConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TPeerConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TRemoteSnapshotStoreOptions
    : public virtual NYTree::TYsonSerializable
{
public:
    int SnapshotReplicationFactor;
    NCompression::ECodec SnapshotCompressionCodec;
    TString SnapshotAccount;
    TString SnapshotPrimaryMedium;
    NErasure::ECodec SnapshotErasureCodec;
    NYTree::IListNodePtr SnapshotAcl;

    TRemoteSnapshotStoreOptions();
};

DEFINE_REFCOUNTED_TYPE(TRemoteSnapshotStoreOptions)

////////////////////////////////////////////////////////////////////////////////

class TRemoteChangelogStoreOptions
    : public virtual NYTree::TYsonSerializable
{
public:
    NErasure::ECodec ChangelogErasureCodec;
    int ChangelogReplicationFactor;
    int ChangelogReadQuorum;
    int ChangelogWriteQuorum;
    bool EnableChangelogMultiplexing;
    bool EnableChangelogChunkPreallocation;
    TString ChangelogAccount;
    TString ChangelogPrimaryMedium;
    NYTree::IListNodePtr ChangelogAcl;

    TRemoteChangelogStoreOptions();
};

DEFINE_REFCOUNTED_TYPE(TRemoteChangelogStoreOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
