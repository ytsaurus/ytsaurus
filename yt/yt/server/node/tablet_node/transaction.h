#pragma once

#include "public.h"
#include "object_detail.h"
#include "dynamic_store_bits.h"

#include <yt/yt/server/lib/lease_server/public.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_detail.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/rpc/authentication_identity.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_BIT_ENUM(ESerializationStatus,
    ((None)                 (0x0000))
    ((CoarseFinished)       (0x0001))
    ((PerRowFinished)       (0x0002))
);

////////////////////////////////////////////////////////////////////////////////

class TTransaction
    : public NTransactionSupervisor::TTransactionBase<TObjectBase>
    , public TRefTracked<TTransaction>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(bool, Transient);
    DEFINE_BYVAL_RW_PROPERTY(bool, Foreign);
    DEFINE_BYVAL_RW_PROPERTY(bool, HasLease);
    DEFINE_BYVAL_RW_PROPERTY(TDuration, Timeout);
    DEFINE_BYVAL_RW_PROPERTY(bool, HasSharedWriteLocks);

    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, StartTimestamp, NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, PrepareTimestamp, NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, CommitTimestamp, NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(NHydra::TRevision, PrepareRevision, NHydra::NullRevision);
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TCellTag, CommitTimestampClusterTag, NObjectClient::InvalidCellTag);
    DEFINE_BYVAL_RW_PROPERTY(int, PartsLeftToPerRowSerialize, 0);
    DEFINE_BYVAL_RW_PROPERTY(ESerializationStatus, SerializationStatus, ESerializationStatus::None);

    DEFINE_BYREF_RW_PROPERTY(THashSet<TTabletId>, TransientAffectedTabletIds);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTabletId>, PersistentAffectedTabletIds);

    DEFINE_BYREF_RW_PROPERTY(THashSet<TTabletId>, CoarseSerializingTabletIds);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTabletId>, PerRowSerializingTabletIds);

    DEFINE_BYREF_RW_PROPERTY(THashSet<TTabletId>, TabletsToUpdateReplicationProgress);
    DEFINE_BYVAL_RW_PROPERTY(bool, CompatSerializationForced);

    DEFINE_BYREF_RW_PROPERTY(TTransactionSignature, PersistentPrepareSignature, InitialTransactionSignature);
    DEFINE_BYREF_RW_PROPERTY(TTransactionSignature, TransientPrepareSignature, InitialTransactionSignature);
    DEFINE_BYVAL_RW_PROPERTY(TTransactionGeneration, PersistentGeneration, InitialTransactionGeneration);
    DEFINE_BYVAL_RW_PROPERTY(TTransactionGeneration, TransientGeneration, InitialTransactionGeneration);

    DEFINE_BYREF_RW_PROPERTY(TTransactionSignature, CommitSignature, InitialTransactionSignature);

    DEFINE_BYREF_RW_PROPERTY(NTransactionSupervisor::TTransactionCommitOptions, CommitOptions);

    DEFINE_BYVAL_RW_PROPERTY(bool, CompatRowsPrepared, false);

    DEFINE_BYREF_RW_PROPERTY(NRpc::TAuthenticationIdentity, AuthenticationIdentity);

    DEFINE_BYREF_RW_PROPERTY(std::vector<NLeaseServer::TLeaseId>, TransientLeaseIds);
    DEFINE_BYREF_RW_PROPERTY(std::vector<NLeaseServer::TLeaseId>, PersistentLeaseIds);

    using TExternalizerTabletMap = THashMap<TTabletId, TTransactionExternalizationToken>;
    DEFINE_BYREF_RW_PROPERTY(TExternalizerTabletMap, ExternalizerTablets);

    DEFINE_BYVAL_RO_PROPERTY(TGuid, ExternalizationToken);

public:
    explicit TTransaction(TTransactionId id);

    virtual ~TTransaction() = default;

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    TFuture<void> GetFinished() const;
    void SetFinished();
    void ResetFinished();

    TTimestamp GetPersistentPrepareTimestamp() const;

    THashSet<TTabletId> GetAffectedTabletIds() const;

    void ForceSerialization(TTabletId tabletId);

    TInstant GetStartTime() const;

    bool IsCoarseSerializationNeeded() const;
    bool IsPerRowSerializationNeeded() const;

    NObjectClient::TCellTag GetCellTag() const;

    bool IsExternalizedToThisCell() const;
    bool IsExternalizedFromThisCell() const;

    void IncrementPartsLeftToPerRowSerialize();
    void DecrementPartsLeftToPerRowSerialize();

private:
    TPromise<void> FinishedPromise_ = NewPromise<void>();
    TFuture<void> FinishedFuture_ = FinishedPromise_.ToFuture().ToUncancelable();
};

////////////////////////////////////////////////////////////////////////////////

class TExternalizedTransaction
    : public TTransaction
{
public:
    explicit TExternalizedTransaction(TTransactionId id, TTransactionExternalizationToken token);

    explicit TExternalizedTransaction(TExternalizedTransactionId id);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
