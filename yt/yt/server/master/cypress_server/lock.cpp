#include "lock.h"
#include "node.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

void TLockKey::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Kind);
    PHOENIX_REGISTER_FIELD(2, Name);
}

PHOENIX_DEFINE_TYPE(TLockKey);

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

void TLockRequest::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Mode);
    PHOENIX_REGISTER_FIELD(2, Key);
    PHOENIX_REGISTER_FIELD(3, Timestamp);
}

PHOENIX_DEFINE_TYPE(TLockRequest);

bool TLockRequest::operator==(const TLockRequest& other) const
{
    return Mode == other.Mode && Key == other.Key;
}

std::strong_ordering TLockRequest::operator<=>(const TLockRequest& other) const
{
    return std::tie(Mode, Key) <=> std::tie(other.Mode, other.Key);
}

void FormatValue(TStringBuilderBase* builder, const TLockRequest& lockRequest, TStringBuf /*spec*/)
{
    builder->AppendFormat("{Mode: %v, Key: %v}", lockRequest.Mode, lockRequest.Key);
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

    if (lhs.first->GetDepth() != rhs.first->GetDepth()) {
        return lhs.first->GetDepth() > rhs.first->GetDepth();
    }

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

    if (lhs->GetDepth() != rhs.first->GetDepth()) {
        return lhs->GetDepth() > rhs.first->GetDepth();
    }

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

    if (lhs.first->GetDepth() != rhs->GetDepth()) {
        return lhs.first->GetDepth() > rhs->GetDepth();
    }

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
    if (lhsTransaction->GetDepth() != rhsTransaction->GetDepth()) {
        return lhsTransaction->GetDepth() > rhsTransaction->GetDepth();
    }

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
    if (lhsTransaction->GetDepth() != rhsTransaction->GetDepth()) {
        return lhsTransaction->GetDepth() > rhsTransaction->GetDepth();
    }

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
    if (lhsTransaction->GetDepth() != rhsTransaction->GetDepth()) {
        return lhsTransaction->GetDepth() > rhsTransaction->GetDepth();
    }

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
    if (lhs->GetDepth() != rhsTransaction->GetDepth()) {
        return lhs->GetDepth() > rhsTransaction->GetDepth();
    }

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
    if (lhsTransaction->GetDepth() != rhs->GetDepth()) {
        return lhsTransaction->GetDepth() > rhs->GetDepth();
    }

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
    return it != TransactionAndKeyToSharedLocks.end() && get<TTransactionRawPtr>(*it) == transaction;
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

void TCypressNodeLockingState::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, AcquiredLocks);
    PHOENIX_REGISTER_FIELD(2, PendingLocks);
}

PHOENIX_DEFINE_TYPE(TCypressNodeLockingState);

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

void TLock::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, Implicit_);
    PHOENIX_REGISTER_FIELD(2, State_);
    PHOENIX_REGISTER_FIELD(3, CreationTime_);
    PHOENIX_REGISTER_FIELD(4, AcquisitionTime_);
    PHOENIX_REGISTER_FIELD(5, Request_);
    PHOENIX_REGISTER_FIELD(6, TrunkNode_,
        .template Serializer<TRawNonversionedObjectPtrSerializer>());
    PHOENIX_REGISTER_FIELD(7, Transaction_);
}

PHOENIX_DEFINE_TYPE(TLock);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
