#include "lock.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

// TODO(cherepashka): remove after corresponding compat in 25.1 will be removed.
DEFINE_ENUM(ECompatLockKeyKind,
    ((None)     (0))
    ((Child)    (1))
    ((Attribute)(2))
);

// TODO(cherepashka): remove after corresponding compat in 25.1 will be removed.
DEFINE_ENUM(ECompatLockState,
    ((Pending)   (0))
    ((Acquired)  (1))
);

////////////////////////////////////////////////////////////////////////////////

void TLockKey::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    if (context.GetVersion() >= EMasterReign::EnumsAndChunkReplicationReductionsInTTableNode) {
        Persist(context, Kind);
    } else {
        Kind = CheckedEnumCast<ELockKeyKind>(Load<ECompatLockKeyKind>(context.LoadContext()));
    }
    Persist(context, Name);
}

void FormatValue(TStringBuilderBase* builder, const TLockKey& key, TStringBuf /*format*/)
{
    if (key.Kind == ELockKeyKind::None) {
        builder->AppendFormat("%v", key.Kind);
    } else {
        builder->AppendFormat("%v[%v]", key.Kind, key.Name);
    }
}

////////////////////////////////////////////////////////////////////////////////

TLockRequest::TLockRequest(ELockMode mode)
    : Mode(mode)
{ }

TLockRequest TLockRequest::MakeSharedChild(TStringBuf key)
{
    TLockRequest result(ELockMode::Shared);
    result.Key.Kind = ELockKeyKind::Child;
    result.Key.Name = key;
    return result;
}

TLockRequest TLockRequest::MakeSharedAttribute(TStringBuf key)
{
    TLockRequest result(ELockMode::Shared);
    result.Key.Kind = ELockKeyKind::Attribute;
    result.Key.Name = key;
    return result;
}

void TLockRequest::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    // COMPAT(cherepashka)
    if (context.GetVersion() >= EMasterReign::EnumsAndChunkReplicationReductionsInTTableNode) {
        Persist(context, Mode);
    } else {
        Mode = CheckedEnumCast<ELockMode>(Load<NCypressClient::ECompatLockMode>(context.LoadContext()));
    }
    Persist(context, Key);
    Persist(context, Timestamp);
}

bool TLockRequest::operator==(const TLockRequest& other) const
{
    return Mode == other.Mode && Key == other.Key;
}

////////////////////////////////////////////////////////////////////////////////

bool TCypressNodeLockingState::TTransactionLockPairComparator::operator()(
    std::pair<TTransaction*, TLock*> lhs,
    std::pair<TTransaction*, TLock*> rhs) const
{
    YT_ASSERT(lhs.first);
    YT_ASSERT(lhs.second);
    YT_ASSERT(rhs.first);
    YT_ASSERT(rhs.second);

    if (lhs.first->GetId() != rhs.first->GetId()) {
        return lhs.first->GetId() < rhs.first->GetId();
    }

    return lhs.second->GetId() < rhs.second->GetId();
}

bool TCypressNodeLockingState::TTransactionLockPairComparator::operator()(
    TTransaction* lhs,
    std::pair<TTransaction*, TLock*> rhs) const
{
    YT_ASSERT(lhs);
    YT_ASSERT(rhs.first);
    YT_ASSERT(rhs.second);

    if (lhs->GetId() != rhs.first->GetId()) {
        return lhs->GetId() < rhs.first->GetId();
    }

    return false;
}

bool TCypressNodeLockingState::TTransactionLockPairComparator::operator()(
    std::pair<TTransaction*, TLock*> lhs,
    TTransaction* rhs) const
{
    YT_ASSERT(lhs.first);
    YT_ASSERT(lhs.second);
    YT_ASSERT(rhs);

    if (lhs.first->GetId() != rhs->GetId()) {
        return lhs.first->GetId() < rhs->GetId();
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

bool TCypressNodeLockingState::TTransactionKeyLockTupleComparator::operator()(
    const std::tuple<TTransaction*, TLockKey, TLock*>& lhs,
    const std::tuple<TTransaction*, TLockKey, TLock*>& rhs) const
{
    YT_ASSERT(get<TTransaction*>(lhs));
    YT_ASSERT(get<TLock*>(lhs));
    YT_ASSERT(get<TTransaction*>(rhs));
    YT_ASSERT(get<TLock*>(rhs));

    const auto* lhsTransaction = get<TTransaction*>(lhs);
    const auto* rhsTransaction = get<TTransaction*>(rhs);
    if (lhsTransaction->GetId() != rhsTransaction->GetId()) {
        return lhsTransaction->GetId() < rhsTransaction->GetId();
    }

    const auto& lhsKey = get<TLockKey>(lhs);
    const auto& rhsKey = get<TLockKey>(rhs);
    if (lhsKey != rhsKey) {
        return lhsKey < rhsKey;
    }

    const auto* lhsLock = get<TLock*>(lhs);
    const auto* rhsLock = get<TLock*>(rhs);
    return lhsLock->GetId() < rhsLock->GetId();
}

bool TCypressNodeLockingState::TTransactionKeyLockTupleComparator::operator()(
    std::pair<TTransaction*, TLockKey> lhs,
    const std::tuple<TTransaction*, TLockKey, TLock*>& rhs) const
{
    YT_ASSERT(lhs.first);
    YT_ASSERT(get<TTransaction*>(rhs));
    YT_ASSERT(get<TLock*>(rhs));

    const auto* lhsTransaction = lhs.first;
    const auto* rhsTransaction = get<TTransaction*>(rhs);
    if (lhsTransaction->GetId() != rhsTransaction->GetId()) {
        return lhsTransaction->GetId() < rhsTransaction->GetId();
    }

    const auto& lhsKey = lhs.second;
    const auto& rhsKey = get<TLockKey>(rhs);
    if (lhsKey != rhsKey) {
        return lhsKey < rhsKey;
    }

    return false;
}

bool TCypressNodeLockingState::TTransactionKeyLockTupleComparator::operator()(
    const std::tuple<TTransaction*, TLockKey, TLock*>& lhs,
    std::pair<TTransaction*, TLockKey> rhs) const
{
    YT_ASSERT(get<TTransaction*>(lhs));
    YT_ASSERT(get<TLock*>(lhs));
    YT_ASSERT(rhs.first);

    const auto* lhsTransaction = get<TTransaction*>(lhs);
    const auto* rhsTransaction = rhs.first;
    if (lhsTransaction->GetId() != rhsTransaction->GetId()) {
        return lhsTransaction->GetId() < rhsTransaction->GetId();
    }

    const auto& lhsKey = get<TLockKey>(lhs);
    const auto& rhsKey = rhs.second;
    if (lhsKey != rhsKey) {
        return lhsKey < rhsKey;
    }

    return false;
}

bool TCypressNodeLockingState::TTransactionKeyLockTupleComparator::operator()(
    const TTransaction* lhs,
    const std::tuple<TTransaction*, TLockKey, TLock*>& rhs) const
{
    YT_ASSERT(lhs);
    YT_ASSERT(get<TTransaction*>(rhs));
    YT_ASSERT(get<TLock*>(rhs));

    const auto* rhsTransaction = get<TTransaction*>(rhs);
    if (lhs->GetId() != rhsTransaction->GetId()) {
        return lhs->GetId() < rhsTransaction->GetId();
    }

    return false;
}

bool TCypressNodeLockingState::TTransactionKeyLockTupleComparator::operator()(
    const std::tuple<TTransaction*, TLockKey, TLock*>& lhs,
    TTransaction* rhs) const
{
    YT_ASSERT(get<TTransaction*>(lhs));
    YT_ASSERT(get<TLock*>(lhs));
    YT_ASSERT(rhs);

    const auto* lhsTransaction = get<TTransaction*>(lhs);
    if (lhsTransaction->GetId() != rhs->GetId()) {
        return lhsTransaction->GetId() < rhs->GetId();
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

bool TCypressNodeLockingState::TKeyLockPairComparator::operator()(
    const std::pair<TLockKey, TLock*>& lhs,
    const std::pair<TLockKey, TLock*>& rhs) const
{
    const auto& lhsLockKey = lhs.first;
    const auto* lhsLock = lhs.second;
    const auto& rhsLockKey = rhs.first;
    const auto* rhsLock = rhs.second;

    YT_ASSERT(lhsLock);
    YT_ASSERT(rhsLock);

    if (auto cmp = lhsLockKey <=> rhsLockKey; cmp != 0) {
        return cmp < 0;
    }

    return lhsLock->GetId() < rhsLock->GetId();
}

bool TCypressNodeLockingState::TKeyLockPairComparator::operator()(
    const TLockKey& lhs,
    const std::pair<TLockKey, TLock*>& rhs) const
{
    const auto& rhsLockKey = rhs.first;
    const auto* rhsLock = rhs.second;

    YT_ASSERT(rhsLock);

    if (auto cmp = lhs <=> rhsLockKey; cmp != 0) {
        return cmp < 0;
    }

    return false;
}

bool TCypressNodeLockingState::TKeyLockPairComparator::operator()(
    const std::pair<TLockKey, TLock*>& lhs,
    const TLockKey& rhs) const
{
    const auto& lhsLockKey = lhs.first;
    const auto* lhsLock = lhs.second;

    YT_ASSERT(lhsLock);

    if (auto cmp = lhsLockKey <=> rhs; cmp != 0) {
        return cmp < 0;
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

bool TCypressNodeLockingState::HasExclusiveLock(TTransaction* transaction) const
{
    auto it = TransactionToExclusiveLocks.lower_bound(transaction);
    return it != TransactionToExclusiveLocks.end() && it->first == transaction;
}

bool TCypressNodeLockingState::HasSharedLock(TTransaction* transaction) const
{
    auto it = TransactionAndKeyToSharedLocks.lower_bound(transaction);
    return it != TransactionAndKeyToSharedLocks.end() && get<TTransaction*>(*it) == transaction;
}

bool TCypressNodeLockingState::HasSnapshotLock(TTransaction* transaction) const
{
    auto it = TransactionToSnapshotLocks.lower_bound(transaction);
    return it != TransactionToSnapshotLocks.end() && it->first == transaction;
}

bool TCypressNodeLockingState::IsEmpty() const
{
    return
        AcquiredLocks.empty() &&
        PendingLocks.empty() &&
        TransactionToExclusiveLocks.empty() &&
        TransactionAndKeyToSharedLocks.empty() &&
        KeyToSharedLocks.empty() &&
        TransactionToSnapshotLocks.empty();
}

void TCypressNodeLockingState::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, AcquiredLocks);
    Persist(context, PendingLocks);
}

const TCypressNodeLockingState TCypressNodeLockingState::Empty = TCypressNodeLockingState();

////////////////////////////////////////////////////////////////////////////////

std::string TLock::GetLowercaseObjectName() const
{
    return Format("lock %v", GetId());
}

std::string TLock::GetCapitalizedObjectName() const
{
    return Format("Lock %v", GetId());
}

void TLock::Save(TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, Implicit_);
    Save(context, State_);
    Save(context, CreationTime_);
    Save(context, AcquisitionTime_);
    Save(context, Request_);
    TRawNonversionedObjectPtrSerializer::Save(context, TrunkNode_);
    Save(context, Transaction_);
}

void TLock::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, Implicit_);

    // COMPAT(cherepashka)
    if (context.GetVersion() >= EMasterReign::EnumsAndChunkReplicationReductionsInTTableNode) {
        Load(context, State_);
    } else {
        State_ = CheckedEnumCast<ELockState>(Load<ECompatLockState>(context));
    }
    Load(context, CreationTime_);
    Load(context, AcquisitionTime_);
    Load(context, Request_);
    TRawNonversionedObjectPtrSerializer::Load(context, TrunkNode_);
    Load(context, Transaction_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

