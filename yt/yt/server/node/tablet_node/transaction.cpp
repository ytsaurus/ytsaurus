#include "transaction.h"

#include "serialize.h"

#include <yt/yt/server/lib/lease_server/lease_manager.h>

#include <yt/yt/server/lib/hydra/hydra_context.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

namespace NYT::NTabletNode {

using namespace NHiveServer;
using namespace NLeaseServer;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(TTransactionId id)
    : TTransactionBase(id)
{ }

void TTransaction::Save(TSaveContext& context) const
{
    TTransactionBase::Save(context);

    using NYT::Save;

    YT_VERIFY(!Transient_);
    Save(context, Foreign_);
    Save(context, Timeout_);
    Save(context, GetPersistentState());
    Save(context, StartTimestamp_);
    Save(context, GetPersistentPrepareTimestamp());
    Save(context, CommitTimestamp_);
    Save(context, PrepareRevision_);
    Save(context, PersistentAffectedTabletIds_);
    Save(context, CoarseSerializingTabletIds_);
    Save(context, PersistentPrepareSignature_);
    Save(context, PersistentGeneration_);
    Save(context, CommitSignature_);
    Save(context, CommitOptions_);
    Save(context, AuthenticationIdentity_.User);
    Save(context, AuthenticationIdentity_.UserTag);
    Save(context, CommitTimestampClusterTag_);
    Save(context, TabletsToUpdateReplicationProgress_);
    Save(context, PersistentLeaseIds_);
    Save(context, ExternalizerTablets_);
    Save(context, ExternalizationToken_);
}

void TTransaction::Load(TLoadContext& context)
{
    TTransactionBase::Load(context);

    using NYT::Load;

    Transient_ = false;
    Load(context, Foreign_);
    Load(context, Timeout_);
    SetPersistentState(Load<ETransactionState>(context));
    Load(context, StartTimestamp_);
    Load(context, PrepareTimestamp_);
    Load(context, CommitTimestamp_);
    Load(context, PrepareRevision_);

    Load(context, PersistentAffectedTabletIds_);
    Load(context, CoarseSerializingTabletIds_);

    Load(context, PersistentPrepareSignature_);
    TransientPrepareSignature_ = PersistentPrepareSignature_;

    Load(context, PersistentGeneration_);
    TransientGeneration_ = PersistentGeneration_;

    Load(context, CommitSignature_);
    Load(context, CommitOptions_);

    Load(context, AuthenticationIdentity_.User);
    Load(context, AuthenticationIdentity_.UserTag);
    Load(context, CommitTimestampClusterTag_);
    Load(context, TabletsToUpdateReplicationProgress_);

    Load(context, PersistentLeaseIds_);

    // COMPAT(ifsmirnov)
    if (context.GetVersion() >= ETabletReign::SmoothMovementForwardWrites) {
        Load(context, ExternalizerTablets_);
    } else {
        if (auto tabletId = Load<TTabletId>(context)) {
            ExternalizerTablets_ = {{tabletId, tabletId}};
        }
    }

    if (context.GetVersion() >= ETabletReign::SmoothMovementForwardWrites) {
        Load(context, ExternalizationToken_);
    }
}

TFuture<void> TTransaction::GetFinished() const
{
    return FinishedFuture_;
}

void TTransaction::SetFinished()
{
    FinishedPromise_.Set();
}

void TTransaction::ResetFinished()
{
    FinishedPromise_.Set();
    FinishedPromise_ = NewPromise<void>();
    FinishedFuture_ = FinishedPromise_.ToFuture().ToUncancelable();
}

TTimestamp TTransaction::GetPersistentPrepareTimestamp() const
{
    switch (GetTransientState()) {
        case ETransactionState::TransientCommitPrepared:
            return NullTimestamp;
        default:
            return PrepareTimestamp_;
    }
}

THashSet<TTabletId> TTransaction::GetAffectedTabletIds() const
{
    THashSet<TTabletId> affectedTabletIds;
    for (auto tabletId : TransientAffectedTabletIds()) {
        affectedTabletIds.insert(tabletId);
    }
    for (auto tabletId : PersistentAffectedTabletIds()) {
        affectedTabletIds.insert(tabletId);
    }

    return affectedTabletIds;
}

void TTransaction::ForceSerialization(TTabletId tabletId)
{
    YT_VERIFY(NHydra::HasHydraContext());

    CoarseSerializingTabletIds_.insert(tabletId);
}

TInstant TTransaction::GetStartTime() const
{
    return TimestampToInstant(StartTimestamp_).first;
}

bool TTransaction::IsCoarseSerializationNeeded() const
{
    return !CoarseSerializingTabletIds_.empty() || !TabletsToUpdateReplicationProgress_.empty();
}

bool TTransaction::IsPerRowSerializationNeeded() const
{
    return !PerRowSerializingTabletIds_.empty();
}

TCellTag TTransaction::GetCellTag() const
{
    return CellTagFromId(GetId());
}

bool TTransaction::IsExternalizedFromThisCell() const
{
    return !ExternalizerTablets_.empty();
}

bool TTransaction::IsExternalizedToThisCell() const
{
    auto type = TypeFromId(Id_);
    return type == EObjectType::ExternalizedSystemTabletTransaction ||
        type == EObjectType::ExternalizedAtomicTabletTransaction ||
        type == EObjectType::ExternalizedNonAtomicTabletTransaction;
}

void TTransaction::IncrementPartsLeftToPerRowSerialize()
{
    ++PartsLeftToPerRowSerialize_;
}

void TTransaction::DecrementPartsLeftToPerRowSerialize()
{
    YT_VERIFY(PartsLeftToPerRowSerialize_ > 0);
    --PartsLeftToPerRowSerialize_;
}

////////////////////////////////////////////////////////////////////////////////

TExternalizedTransaction::TExternalizedTransaction(TTransactionId id, TTransactionExternalizationToken token)
    : TTransaction(id)
{
    ExternalizationToken_ = token;
};

TExternalizedTransaction::TExternalizedTransaction(TExternalizedTransactionId id)
    : TExternalizedTransaction(id.first, id.second)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

