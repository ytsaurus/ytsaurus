#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/erasure/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TPeerConnectionConfig
    : public NRpc::TBalancingChannelConfig
{
public:
    TCellId CellId;

    //! If true, Hydra peer state is ignored during
    //! discovery.
    //! This is used for job proxies where master addresses
    //! are overridden with master cache addresses.
    bool IgnorePeerState;

    REGISTER_YSON_STRUCT(TPeerConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPeerConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TRemoteSnapshotStoreOptions
    : public virtual NYTree::TYsonStruct
{
public:
    int SnapshotReplicationFactor;
    NCompression::ECodec SnapshotCompressionCodec;
    TString SnapshotAccount;
    TString SnapshotPrimaryMedium;
    NErasure::ECodec SnapshotErasureCodec;
    bool SnapshotEnableStripedErasure;
    NYTree::IListNodePtr SnapshotAcl;

    REGISTER_YSON_STRUCT(TRemoteSnapshotStoreOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRemoteSnapshotStoreOptions)

////////////////////////////////////////////////////////////////////////////////

class TRemoteChangelogStoreOptions
    : public virtual NYTree::TYsonStruct
{
public:
    NErasure::ECodec ChangelogErasureCodec;
    int ChangelogReplicationFactor;
    int ChangelogReadQuorum;
    int ChangelogWriteQuorum;
    bool EnableChangelogMultiplexing;
    bool EnableChangelogChunkPreallocation;
    i64 ChangelogReplicaLagLimit;
    std::optional<NObjectClient::TCellTag> ChangelogExternalCellTag;
    TString ChangelogAccount;
    TString ChangelogPrimaryMedium;
    NYTree::IListNodePtr ChangelogAcl;

    REGISTER_YSON_STRUCT(TRemoteChangelogStoreOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRemoteChangelogStoreOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
