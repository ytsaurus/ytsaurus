#pragma once

#include "persisted_state.h"
#include "public.h"

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(EStorageRowFlags, ui64,
    ((None)      (0x0000))
    ((Commit)    (0x0001))
);

////////////////////////////////////////////////////////////////////////////////

// Examples of serializer that can be specified as TDefaultSerializer in TPersistedStateControl
//  or as TSerializer in TPersistedStateControl::CreateState.

// Simple universal serializer that uses Yson library.
// class TSerializerExample
// {
// public:
//     template <class T>
//     std::string Encode(const std::optional<T>& value) const
//     {
//         return value.has_value() ? ConvertToYsonString(value.value()).ToString() : "";
//     }
//     template <class T>
//     std::optional<T> Decode(const std::string& serial, std::type_identity<T>) const
//     {
//         return serial.empty() ? std::optional<T>(std::nullopt) : std::optional<T>(ConvertTo<T>(TYsonString(serial)));
//     }
// };

// Generic serializer that uses only standard library.
// Let TKey = int, TValue = std::string; TDBKey = TDBValue = std::string.
// class TSerializerExample
// {
// public:
//     std::string Encode(const std::optional<int>& value) const
//     {
//         return value.has_value() ? std::to_string(value.value()) : "";
//     }
//
//     std::string Encode(const std::optional<std::string>& value) const
//     {
//         return value.has_value() ? "!" + value.value() : "";
//     }
//
//     std::optional<int> Decode(const std::string& serial, std::type_identity<int>) const
//     {
//         return serial.empty() ? std::optional<int>{std::nullopt} : std::optional<int>{std::stoi(serial)};
//     }
//
//     std::optional<std::string> Decode(const std::string& serial, std::type_identity<std::string>) const
//     {
//         return serial.empty() ? std::optional<std::string>{std::nullopt} : std::optional<std::string>{serial.substr(1)};
//     }
// };

////////////////////////////////////////////////////////////////////////////////

//! Default empty serializer.
//! If used, a correct serializer must be provided at each TPersistedStateControl::CreateState call.
class TSpecifySerializerInCreateState
{ };

////////////////////////////////////////////////////////////////////////////////

template <class TDBKey, class TDBValue = TDBKey, class TDefaultSerializer = TSpecifySerializerInCreateState>
class TPersistedStateControl;

////////////////////////////////////////////////////////////////////////////////

//! Row in database table.
template <class TDBKey, class TDBValue = TDBKey>
struct TPersistedStateStorageRow
{
    TSequenceId SequenceId;
    EStorageRowFlags Flags = EStorageRowFlags::None;
    TPersistedStateName Name;
    TDBKey KeyLeft;
    TDBKey KeyRight;
    TDBValue Value;
    bool operator==(const TPersistedStateStorageRow&) const = default;
};

//! Abstract database table handler.
template <class TDBKey, class TDBValue = TDBKey>
class TPersistedStateStorageHandlerBase
    : public TRefCounted
{
public:
    using TStorageRow = TPersistedStateStorageRow<TDBKey, TDBValue>;

    //! Maximal number of rows that can be read by one select.
    DEFINE_BYVAL_RO_PROPERTY(ssize_t, MaxSelectSize, 1);

    //! Maximal allowed number of statements in one transaction.
    DEFINE_BYVAL_RO_PROPERTY(ssize_t, MaxExecuteSize, std::numeric_limits<ssize_t>::max());

    //! Get some rows with SequenceId > lastSequenceId, ordered by SequenceId.
    //! The rows are appended to given 'rows' vector.
    //! Note that MaxSelectSize is the maximal number of rows can be selected at once.
    // So if exactly MaxSelectSize were selected (and appended to 'rows') in one call then
    // a consequent Select call(s) must be issued until appended amount is less than MaxSelectSize.
    virtual void Select(TSequenceId lastSequenceId, std::vector<TStorageRow>& rows) = 0;

    //! Execute given statements in one transaction.
    virtual void Execute(
        std::vector<TStorageRow>&& insertRows,
        const std::vector<TSequenceId>& deleteRows,
        bool isFinal,
        const std::vector<TPersistedStateCommitContext*>& commitContexts) = 0;
};

template <class TDBKey, class TDBValue>
using TStorageHandlerBasePtr = TIntrusivePtr<TPersistedStateStorageHandlerBase<TDBKey, TDBValue>>;

////////////////////////////////////////////////////////////////////////////////

//! In-memory storage handler that persists nothing and reads nothing back. It lets a control act as
//! a leader (so it can Recover() and run write transactions) without touching any real database, e.g.
//! to materialize a detached, queryable snapshot from an external source. Recover() yields an empty
//! state, so the caller must populate the control itself.
template <class TDBKey, class TDBValue = TDBKey>
class TNullStorageHandler
    : public TPersistedStateStorageHandlerBase<TDBKey, TDBValue>
{
public:
    using TStorageRow = typename TPersistedStateStorageHandlerBase<TDBKey, TDBValue>::TStorageRow;

    void Select(TSequenceId /*lastSequenceId*/, std::vector<TStorageRow>& /*rows*/) override
    { }

    void Execute(
        std::vector<TStorageRow>&& /*insertRows*/,
        const std::vector<TSequenceId>& /*deleteRows*/,
        bool /*isFinal*/,
        const std::vector<TPersistedStateCommitContext*>& /*commitContexts*/) override
    { }
};

////////////////////////////////////////////////////////////////////////////////

//! Base of TPersistedStateImpl that does not depend on particular TKey and TValue.
template <class TDBKey, class TDBValue = TDBKey>
class IPersistedStateImpl;

//! Connection between SequenceId, the state that owns a record with that ID and the record itself.
template <class TDBKey, class TDBValue>
struct TPersistedStateKnownRecord;

//! Connection between SequenceId, the state that owns a record with that ID and the record itself.
template <class TDBKey, class TDBValue>
struct TPersistedStateKnownRecordLess;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TPersistedStateCheckpoint);

////////////////////////////////////////////////////////////////////////////////

struct TPersistedStateControlBase
    : public TRefCounted
{ };

////////////////////////////////////////////////////////////////////////////////

template <class TDBKey, class TDBValue, class TDefaultSerializer>
class TPersistedStateTransaction;

////////////////////////////////////////////////////////////////////////////////

//! Central point of several (perhaps one) instances of TPersistedState.
//! Implements all communication with database and between leader and replica.
template <class TDBKey, class TDBValue, class TDefaultSerializer>
class TPersistedStateControl
    : public TPersistedStateControlBase
{
public:
    using TStorageHandlerPtr = TStorageHandlerBasePtr<TDBKey, TDBValue>;
    using TStorageRow = TPersistedStateStorageRow<TDBKey, TDBValue>;

private:
    using TKnownRecord = TPersistedStateKnownRecord<TDBKey, TDBValue>;
    using TKnownRecordLess = TPersistedStateKnownRecordLess<TDBKey, TDBValue>;
    using TPersistedStateImplPtr = TIntrusivePtr<IPersistedStateImpl<TDBKey, TDBValue>>;
    using TStateMap = THashMap<TPersistedStateName, TPersistedStateImplPtr>;
    using TKnownRecordsSet = std::set<TKnownRecord, TKnownRecordLess>;
    using TTransaction = TPersistedStateTransaction<TDBKey, TDBValue, TDefaultSerializer>;

    //! Database handler. Must be set for leader control but not for replica.
    const TStorageHandlerPtr Storage_;
    //! Flag that is set to true after a successful recover.
    bool IsRecovered_ = false;
    //! Last known sequence ID. Exactly the last (maximal) sequence ID in KnownRecords_.
    TSequenceId LastKnownId_ = FakeSequenceId;
    //! Last confirmed (committed and then successfully persisted) sequence ID.
    TSequenceId ConfirmedId_ = FakeSequenceId;
    //! List of states that belong to this control.
    TStateMap States_;
    //! List of all records ordered by SequenceId.
    TKnownRecordsSet KnownRecords_;
    //! Sequence IDs that must be deleted from database before any other database modification.
    std::deque<TSequenceId> GarbageIds_;
    //! List describing transactional states of the persisted state.
    TIntrusiveList<TPersistedStateCheckpoint> CheckpointList_;
    //! Pointer holder of transactional states.
    std::deque<TPersistedStateCheckpointPtr> CheckpointDeque_;
    //! The mutex that is locked in CommitTransaction.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, TransactionalLock_);
    bool PersistInProgress_ = false;
    std::vector<TPersistedStateTransactionPtr> PersistPendingTransactions_;
    ssize_t PersistFailureEpoch_ = 0;

public:
    //! Create control of states. If storage is not passed - the control in intended to be a replica of some other leader.
    explicit TPersistedStateControl(TStorageHandlerPtr storage = nullptr);

    //! Destroy control. Owned states are destroyed or becomes unusable orphans.
    ~TPersistedStateControl() override;

    TIntrusivePtr<TPersistedStateControl> Clone();

    //! Register new state, a simple table with key and values columns (key is unique).
    template <class TKey, class TValue, class TSerializer = TDefaultSerializer>
    TPersistedStatePtr<TKey, TValue> CreateState(const TPersistedStateName& name, const TSerializer& serializer = {});

    template <class TKey, class TValue>
    TPersistedStatePtr<TKey, TValue> CloneState(const TPersistedStatePtr<TKey, TValue>& state);

    //! Load data from database.
    void Recover();
    //! Apply a portion of rows.
    void Apply(const std::vector<TStorageRow>& rows, std::deque<TSequenceId>* droppedIds);
    //! Get the greatest known sequence ID.
    TSequenceId GetSequenceId() const;
    //! Get a portion of rows for a replica to apply with.
    std::vector<TStorageRow> Follow(TSequenceId sequenceIdFrom, ssize_t limit = std::numeric_limits<ssize_t>::max());

    //! Create snapshot of all states and start a new consistent transaction.
    //! If `copyWritesFrom` is set (must be a transaction of this control), the new transaction is seeded with a
    //! copy of that transaction's write set, so a read-only snapshot can observe still-uncommitted changes.
    TPersistedStateTransactionPtr StartTransaction(bool readOnly = false, const TPersistedStateTransactionPtr& copyWritesFrom = nullptr);

private:
    template <class TKey, class TValue, class TSerializer, class TAnyDBKey, class TAnyDBValue, class TAnyDefaultSerializer>
    friend class TPersistedStateImpl;

    template <class TAnyDBKey, class TAnyDBValue, class TAnyDefaultSerializer>
    friend class TPersistedStateTransaction;

    template <class TAnyDBKey, class TAnyDBValue>
    friend class IPersistedStateWrapper;

    //! Implementation of TPersistedStateTransaction::Commit.
    void CommitTransaction(TTransaction& transaction, TPersistedStateCommitContext* commitContext);

    //! Implementation of TPersistedStateTransaction::Abort.
    void AbortTransaction(TTransaction& transaction);

    //! Call ValidateTransaction for all states.
    void ValidateTransaction(TTransaction& transaction, TGuard<NThreading::TSpinLock>& guard);

    //! Call PrepareTransaction for all states.
    void PrepareTransaction(TTransaction& transaction, TPersistedStateCommitContext* commitContext);

    //! Undo all changes that were made in CommitTransaction.
    void RevertTransaction(TTransaction& transaction);

    //! Undo all changes that were made in CommitTransaction.
    void CompleteTransaction(TTransaction& transaction);

    //! Take into account given changes in persisted state (that's probably Recover in master or Apply in replica).
    //! That is just save add |introduceRecords| and remove |deleteRecords| from KnownRecords_.
    void Notify(
        std::vector<TKnownRecord>&& introduceRecords,
        const std::vector<TSequenceId>& deleteRecords);
    //! Persist all pending transactions.
    void Persist();

    //! Apply row, for example apply row on replica upon receiving the row from master.
    void Apply(const TStorageRow& row, std::deque<TSequenceId>* droppedIds);

    //! Helpers for managing checkpoints.
    void RegisterAsReader(TTransaction& transaction, TGuard<NThreading::TSpinLock>& guard);
    void UnregisterAsReader(TTransaction& transaction, TGuard<NThreading::TSpinLock>& guard);
    void RegisterAsWriter(TTransaction& transaction, TGuard<NThreading::TSpinLock>& guard);
    void CleanupCheckpoints(TGuard<NThreading::TSpinLock>& guard);
};

template <class TDBKey, class TDBValue = TDBKey, class TDefaultSerializer = TSpecifySerializerInCreateState>
using TPersistedStateControlPtr = TIntrusivePtr<TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define PERSISTED_STATE_CONTROL_H_
#include "persisted_state_control-inl.h"
#undef PERSISTED_STATE_CONTROL_H_
