#include "transaction.h"
#include "automaton.h"
#include "sorted_dynamic_store.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NTabletNode {

using namespace NHiveServer;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TTransactionWriteRecord::TTransactionWriteRecord(
    TTabletId tabletId,
    TSharedRef data,
    int rowCount,
    i64 dataWeight,
    const TSyncReplicaIdList& syncReplicaIds,
    const std::optional<NTableClient::THunkChunksInfo>& hunkChunksInfo)
    : TabletId(tabletId)
    , Data(std::move(data))
    , RowCount(rowCount)
    , DataWeight(dataWeight)
    , SyncReplicaIds(syncReplicaIds)
    , HunkChunksInfo(hunkChunksInfo)
{ }

void TTransactionWriteRecord::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, TabletId);
    Save(context, Data);
    Save(context, RowCount);
    Save(context, DataWeight);
    Save(context, SyncReplicaIds);
    Save(context, HunkChunksInfo);
}

void TTransactionWriteRecord::Load(TLoadContext& context)
{
    using NYT::Load;
    Load(context, TabletId);
    Load(context, Data);
    Load(context, RowCount);
    Load(context, DataWeight);
    Load(context, SyncReplicaIds);

    // COMPAT(aleksandra-zh)
    if (context.GetVersion() >= ETabletReign::JournalHunks) {
        Load(context, HunkChunksInfo);
    }
}

i64 TTransactionWriteRecord::GetByteSize() const
{
    return Data.Size();
}

i64 GetWriteLogRowCount(const TTransactionWriteLog& writeLog)
{
    i64 result = 0;
    for (const auto& entry : writeLog) {
        result += entry.RowCount;
    }
    return result;
}

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
    Save(context, SerializingTabletIds_);
    Save(context, PersistentPrepareSignature_);
    Save(context, PersistentGeneration_);
    Save(context, CommitSignature_);
    Save(context, CommitOptions_);
    Save(context, AuthenticationIdentity_.User);
    Save(context, AuthenticationIdentity_.UserTag);
    Save(context, CommitTimestampClusterTag_);
    Save(context, TabletsToUpdateReplicationProgress_);
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

    // COMPAT(gritukan)
    if (context.GetVersion() >= ETabletReign::TabletWriteManager) {
        Load(context, PersistentAffectedTabletIds_);
        Load(context, SerializingTabletIds_);
    }

    Load(context, PersistentPrepareSignature_);
    TransientPrepareSignature_ = PersistentPrepareSignature_;

    Load(context, PersistentGeneration_);
    TransientGeneration_ = PersistentGeneration_;

    Load(context, CommitSignature_);
    Load(context, CommitOptions_);

    // COMPAT(gritukan)
    if (context.GetVersion() < ETabletReign::TabletWriteManager) {
        Load(context, CompatRowsPrepared_);
    }
    Load(context, AuthenticationIdentity_.User);
    Load(context, AuthenticationIdentity_.UserTag);
    Load(context, CommitTimestampClusterTag_);
    // COMPAT(gritukan)
    if (context.GetVersion() < ETabletReign::TabletWriteManager) {
        Load(context, CompatSerializationForced_);
    }
    Load(context, TabletsToUpdateReplicationProgress_);
}

void TTransaction::AsyncLoad(TLoadContext& context)
{
    using NYT::Load;
    // COMPAT(gritukan)
    if (context.GetVersion() <= ETabletReign::TabletWriteManager) {
        Load(context, CompatImmediateLockedWriteLog_);
        Load(context, CompatImmediateLocklessWriteLog_);
        Load(context, CompatDelayedLocklessWriteLog_);
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

    SerializingTabletIds_.insert(tabletId);
}

TInstant TTransaction::GetStartTime() const
{
    return TimestampToInstant(StartTimestamp_).first;
}

bool TTransaction::IsSerializationNeeded() const
{
    return !SerializingTabletIds_.empty() || !TabletsToUpdateReplicationProgress_.empty();
}

TCellTag TTransaction::GetCellTag() const
{
    return CellTagFromId(GetId());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

