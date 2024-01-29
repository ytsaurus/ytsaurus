#include "lock.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

bool TLockKey::operator ==(const TLockKey& other) const
{
    return Kind == other.Kind && Name == other.Name;
}

bool TLockKey::operator <(const TLockKey& other) const
{
    return std::tie(Kind, Name) < std::tie(other.Kind, other.Name);
}

TLockKey::operator size_t() const
{
    return MultiHash(Kind, Name);
}

void TLockKey::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Kind);
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

TLockRequest TLockRequest::MakeSharedChild(const TString& key)
{
    TLockRequest result(ELockMode::Shared);
    result.Key.Kind = ELockKeyKind::Child;
    result.Key.Name = key;
    return result;
}

TLockRequest TLockRequest::MakeSharedAttribute(const TString& key)
{
    TLockRequest result(ELockMode::Shared);
    result.Key.Kind = ELockKeyKind::Attribute;
    result.Key.Name = key;
    return result;
}

void TLockRequest::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Mode);
    Persist(context, Key);
    Persist(context, Timestamp);
}

bool TLockRequest::operator==(const TLockRequest& other) const
{
    return Mode == other.Mode && Key == other.Key;
}

////////////////////////////////////////////////////////////////////////////////

bool TCypressNodeLockingState::TLockEntryComparator::operator()(
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

bool TCypressNodeLockingState::TLockEntryComparator::operator()(
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

bool TCypressNodeLockingState::TLockEntryComparator::operator()(
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

bool TCypressNodeLockingState::TLockEntryComparator::operator()(
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

bool TCypressNodeLockingState::TLockEntryComparator::operator()(
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

bool TCypressNodeLockingState::TLockEntryComparator::operator()(
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

bool TCypressNodeLockingState::TLockEntryComparator::operator()(
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

bool TCypressNodeLockingState::TLockEntryComparator::operator()(
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

TString TLock::GetLowercaseObjectName() const
{
    return Format("lock %v", GetId());
}

TString TLock::GetCapitalizedObjectName() const
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
    Load(context, State_);
    Load(context, CreationTime_);
    Load(context, AcquisitionTime_);
    Load(context, Request_);
    TRawNonversionedObjectPtrSerializer::Load(context, TrunkNode_);
    Load(context, Transaction_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

