#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/ref_tracked.h>

#include <util/generic/hash_multi_map.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct TLockKey
{
    ELockKeyKind Kind = ELockKeyKind::None;
    TString Name;

    bool operator ==(const TLockKey& other) const;
    bool operator !=(const TLockKey& other) const;
    bool operator < (const TLockKey& other) const;
    operator size_t() const;

    void Persist(const NCellMaster::TPersistenceContext& context);
};

void FormatValue(TStringBuilderBase* builder, const TLockKey& key, TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TLockRequest
{
    TLockRequest() = default;
    TLockRequest(ELockMode mode);

    static TLockRequest MakeSharedChild(const TString& key);
    static TLockRequest MakeSharedAttribute(const TString& key);

    void Persist(const NCellMaster::TPersistenceContext& context);

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
    struct TLockEntryComparator
    {
        using is_transparent = void;

        // Transaction-lock pair comparisons.

        bool operator()(
            std::pair<NTransactionServer::TTransaction*, TLock*> lhs,
            std::pair<NTransactionServer::TTransaction*, TLock*> rhs) const;

        bool operator()(
            NTransactionServer::TTransaction* lhs,
            std::pair<NTransactionServer::TTransaction*, TLock*> rhs) const;

        bool operator()(
            std::pair<NTransactionServer::TTransaction*, TLock*> lhs,
            NTransactionServer::TTransaction* rhs) const;

        // Transaction-key-lock tuple comparisons.

        bool operator()(
            const std::tuple<NTransactionServer::TTransaction*, TLockKey, TLock*>& lhs,
            const std::tuple<NTransactionServer::TTransaction*, TLockKey, TLock*>& rhs) const;

        bool operator()(
            std::pair<NTransactionServer::TTransaction*, TLockKey> lhs,
            const std::tuple<NTransactionServer::TTransaction*, TLockKey, TLock*>& rhs) const;

        bool operator()(
            const std::tuple<NTransactionServer::TTransaction*, TLockKey, TLock*>& lhs,
            std::pair<NTransactionServer::TTransaction*, TLockKey> rhs) const;

        bool operator()(
            const NTransactionServer::TTransaction* lhs,
            const std::tuple<NTransactionServer::TTransaction*, TLockKey, TLock*>& rhs) const;

        bool operator()(
            const std::tuple<NTransactionServer::TTransaction*, TLockKey, TLock*>& lhs,
            NTransactionServer::TTransaction* rhs) const;
    };

    std::list<TLock*> AcquiredLocks;
    std::list<TLock*> PendingLocks;

    // NB: iterators to these containers are stored in corresponding TLock objects.
    // They must not be invalidated unless the record itself is erased. Keep this
    // in mind when changing container types.
    // NB: deterministic order for both keys and values is required here, hence std::set.
    std::set<std::pair<NTransactionServer::TTransaction*, TLock*>, TLockEntryComparator> TransactionToExclusiveLocks;

    std::set<std::tuple<NTransactionServer::TTransaction*, TLockKey, TLock*>, TLockEntryComparator> TransactionAndKeyToSharedLocks;
    // Only contains "child" and "attribute" shared locks.
    THashMultiMap<TLockKey, TLock*> KeyToSharedLocks;

    std::set<std::pair<NTransactionServer::TTransaction*, TLock*>, TLockEntryComparator> TransactionToSnapshotLocks;

    bool HasExclusiveLock(NTransactionServer::TTransaction* transaction) const;
    bool HasSharedLock(NTransactionServer::TTransaction* transaction) const;
    bool HasSnapshotLock(NTransactionServer::TTransaction* transaction) const;

    bool IsEmpty() const;
    void Persist(const NCellMaster::TPersistenceContext& context);

    static const TCypressNodeLockingState Empty;
};

////////////////////////////////////////////////////////////////////////////////

//! Describes a lock (either held or waiting).
class TLock
    : public NObjectServer::TObject
    , public TRefTracked<TLock>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(bool, Implicit);
    DEFINE_BYVAL_RW_PROPERTY(ELockState, State, ELockState::Pending);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, CreationTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, AcquisitionTime);
    DEFINE_BYREF_RW_PROPERTY(TLockRequest, Request);
    DEFINE_BYVAL_RW_PROPERTY(TCypressNode*, TrunkNode);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, Transaction);

    // Not persisted.
    using TLockListIterator = std::list<TLock*>::iterator;
    DEFINE_BYVAL_RW_PROPERTY(TLockListIterator, LockListIterator);

    using TTransactionToExclusiveLocksIterator = decltype(TCypressNodeLockingState::TransactionToExclusiveLocks)::iterator;
    DEFINE_BYVAL_RW_PROPERTY(TTransactionToExclusiveLocksIterator, TransactionToExclusiveLocksIterator);

    using TTransactionAndKeyToSharedLocksIterator = decltype(TCypressNodeLockingState::TransactionAndKeyToSharedLocks)::iterator;
    DEFINE_BYVAL_RW_PROPERTY(TTransactionAndKeyToSharedLocksIterator, TransactionAndKeyToSharedLocksIterator);

    using TKeyToSharedLocksIterator = THashMultiMap<TLockKey, TLock*>::iterator;
    DEFINE_BYVAL_RW_PROPERTY(TKeyToSharedLocksIterator, KeyToSharedLocksIterator);

    using TTransactionToSnapshotLocksIterator = decltype(TCypressNodeLockingState::TransactionToSnapshotLocks)::iterator;
    DEFINE_BYVAL_RW_PROPERTY(TTransactionToSnapshotLocksIterator, TransactionToSnapshotLocksIterator);

public:
    using TObject::TObject;

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
