#pragma once

#include "public.h"

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EReplicaContentType,
    ((Data)     (0))
    ((Queue)    (1))
    ((External) (2))
);

DEFINE_ENUM(EReplicaMode,
    ((Sync)           (0))
    ((Async)          (1))
    ((AsyncToSync)    (2))
    ((SyncToAsync)    (3))
);

DEFINE_ENUM(EReplicaState,
    ((Disabled)             (0))
    ((Enabled)              (1))
    ((Disabling)            (2))
    ((Enabling)             (3))
);

bool IsStableReplicaMode(EReplicaMode mode);
bool IsStableReplicaState(EReplicaState state);

///////////////////////////////////////////////////////////////////////////////

struct TReplicationProgress
{
    struct TSegment
    {
        NTableClient::TUnversionedOwningRow LowerKey;
        NTransactionClient::TTimestamp Timestamp;

        void Persist(const TStreamPersistenceContext& context);
    };

    std::vector<TSegment> Segments;
    NTableClient::TUnversionedOwningRow UpperKey;

    void Persist(const TStreamPersistenceContext& context);
};

struct TReplicaHistoryItem
{
    NChaosClient::TReplicationEra Era;
    NTransactionClient::TTimestamp Timestamp;
    NChaosClient::EReplicaMode Mode;
    NChaosClient::EReplicaState State;

    void Persist(const TStreamPersistenceContext& context);
};

struct TReplicaInfo
{
    TReplicaId ReplicaId;
    TString Cluster;
    NYPath::TYPath TablePath;
    EReplicaContentType ContentType;
    EReplicaMode Mode;
    EReplicaState State;
    TReplicationProgress ReplicationProgress;
    std::vector<TReplicaHistoryItem> History;

    void Persist(const TStreamPersistenceContext& context);
};

struct TReplicationCard
    : public TRefCounted
{
    std::vector<TReplicaInfo> Replicas;
    std::vector<NObjectClient::TCellId> CoordinatorCellIds;
    TReplicationEra Era;
};

DEFINE_REFCOUNTED_TYPE(TReplicationCard)

struct TReplicationCardToken
{
    NObjectClient::TCellId ChaosCellId;
    TReplicationCardId ReplicationCardId;

    TReplicationCardToken() = default;
    TReplicationCardToken(
        NObjectClient::TCellId chaosCellId,
        TReplicationCardId replicationCardId);

    operator size_t() const;
    explicit operator bool() const;
    bool operator == (const TReplicationCardToken& other) const;
};

///////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TReplicationProgress& replicationProgress, TStringBuf /*spec*/);
TString ToString(const TReplicationProgress& replicationProgress);

void FormatValue(TStringBuilderBase* builder, const TReplicaInfo& replicaInfo, TStringBuf /*spec*/);
TString ToString(const TReplicaInfo& replicaInfo);

void FormatValue(TStringBuilderBase* builder, const TReplicationCard& replicationCard, TStringBuf /*spec*/);
TString ToString(const TReplicationCard& replicationCard);

void FormatValue(TStringBuilderBase* builder, const TReplicationCardToken& replicationCardToken, TStringBuf /*spec*/);
TString ToString(const TReplicationCardToken& replicationCardToken);

////////////////////////////////////////////////////////////////////////////////

void UpdateReplicationProgress(TReplicationProgress* progress, const TReplicationProgress& update);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

