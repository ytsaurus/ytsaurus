#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/server/transaction_server/public.h>

#include <yt/core/actions/signal.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

#include <util/generic/map.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct TLockKey
{
    ELockKeyKind Kind = ELockKeyKind::None;
    TString Name;

    bool operator ==(const TLockKey& other) const;
    bool operator !=(const TLockKey& other) const;
    bool operator < (const TLockKey& other) const;
    operator size_t() const;

    void Persist(NCellMaster::TPersistenceContext& context);
};

void FormatValue(TStringBuilder* builder, const TLockKey& key, TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TLockRequest
{
    TLockRequest() = default;
    TLockRequest(ELockMode mode);

    static TLockRequest MakeSharedChild(const TString& key);
    static TLockRequest MakeSharedAttribute(const TString& key);

    void Persist(NCellMaster::TPersistenceContext& context);

    bool operator == (const TLockRequest& other) const;
    bool operator != (const TLockRequest& other) const;

    ELockMode Mode;
    TLockKey Key;
    NTransactionClient::TTimestamp Timestamp = NTransactionClient::NullTimestamp;
};

////////////////////////////////////////////////////////////////////////////////

//! Describes the locking state of a Cypress node.
struct TCypressNodeLockingState
{
    std::list<TLock*> AcquiredLocks;
    std::list<TLock*> PendingLocks;
    // NB: We rely on THash* containers not to invalidate iterators on rehash.
    // Keep this in mind when replacing them with std::* analogues.
    THashMultiMap<NTransactionServer::TTransaction*, TLock*> TransactionToExclusiveLocks;
    THashMultiMap<std::pair<NTransactionServer::TTransaction*, TLockKey>, TLock*> TransactionAndKeyToSharedLocks;
    // Only contains "child" and "attribute" shared locks.
    THashMultiMap<TLockKey, TLock*> KeyToSharedLocks;
    THashMultiMap<NTransactionServer::TTransaction*, TLock*> TransactionToSnapshotLocks;

    bool IsEmpty() const;
    void Persist(NCellMaster::TPersistenceContext& context);

    static const TCypressNodeLockingState Empty;
};

////////////////////////////////////////////////////////////////////////////////

//! Describes a lock (either held or waiting).
class TLock
    : public NObjectServer::TNonversionedObjectBase
    , public TRefTracked<TLock>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(bool, Implicit);
    DEFINE_BYVAL_RW_PROPERTY(ELockState, State, ELockState::Pending);
    DEFINE_BYREF_RW_PROPERTY(TLockRequest, Request);
    DEFINE_BYVAL_RW_PROPERTY(TCypressNodeBase*, TrunkNode);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, Transaction);
    
    // Not persisted.
    using TLockListIterator = std::list<TLock*>::iterator;
    DEFINE_BYVAL_RW_PROPERTY(TLockListIterator, LockListIterator);

    using TTransactionToExclusiveLocksIterator = THashMultiMap<NTransactionServer::TTransaction*, TLock*>::iterator;
    DEFINE_BYVAL_RW_PROPERTY(TTransactionToExclusiveLocksIterator, TransactionToExclusiveLocksIterator);

    using TTransactionAndKeyToSharedLocksIterator = THashMultiMap<std::pair<NTransactionServer::TTransaction*, TLockKey>, TLock*>::iterator;
    DEFINE_BYVAL_RW_PROPERTY(TTransactionAndKeyToSharedLocksIterator, TransactionAndKeyToSharedLocksIterator);

    using TKeyToSharedLocksIterator = THashMultiMap<TLockKey, TLock*>::iterator;
    DEFINE_BYVAL_RW_PROPERTY(TKeyToSharedLocksIterator, KeyToSharedLocksIterator);

    using TTransactionToSnapshotLocksIterator = THashMultiMap<NTransactionServer::TTransaction*, TLock*>::iterator;
    DEFINE_BYVAL_RW_PROPERTY(TTransactionToSnapshotLocksIterator, TransactionToSnapshotLocksIterator);

public:
    explicit TLock(const TLockId& id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
