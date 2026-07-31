#pragma once

#ifndef PERSISTED_STATE_CONTROL_H_
    #error "Direct inclusion of this file is not allowed, include persisted_state_control.h"
    // For the sake of sane code completion.
    #include "persisted_state_control.h"
#endif
#include "private.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Base of TPersistedStateImpl that does not depend on particular TKey and TValue.
template <class TDBKey, class TDBValue>
class IPersistedStateImpl
    : public virtual TRefCounted
{
public:
    using TKnownRecord = TPersistedStateKnownRecord<TDBKey, TDBValue>;
    using TStorageRow = TPersistedStateStorageRow<TDBKey, TDBValue>;

    //! Redirect row to TPersistedState::Apply.
    virtual void Apply(
        TSequenceId sequenceId,
        const TDBKey& keyLeft,
        const TDBKey& keyRight,
        const TDBValue& value,
        std::deque<TSequenceId>* droppedIds) = 0;
    //! Redirect row to TPersistedState::CheckContents.
    virtual void CheckContents(const TPersistedStateName& name) const = 0;

    //! Check that the transaction has no conflicts with earlier committed transactions.
    virtual void ValidateTransaction(TPersistedStateTransactionBase& tx, TPersistedStateTransactionContextBase& context) = 0;
    //! Redirect to TPersistedState::PrepareTransaction.
    virtual void PrepareTransaction(
        TPersistedStateTransactionContextBase& context,
        std::vector<TKnownRecord>& introduceRecords,
        std::vector<TStorageRow>& insertRows,
        std::vector<TSequenceId>& deleteRecords) = 0;
    //! Redirect to TPersistedState::RevertTransaction.
    virtual void RevertTransaction(TPersistedStateTransactionContextBase& context) = 0;
    //! Redirect to TPersistedState::CompleteTransaction.
    virtual void CompleteTransaction(TPersistedStateTransactionContextBase& context) = 0;
    //! Create transaction context base.
    virtual TPersistedStateTransactionContextPtr CreateTransactionContextBase(TPersistedStateTransactionBase& tx, bool readOnly) = 0;
    //! Copy this state's write set (and record-set size diff) from srcTx's context into dstTx's context.
    virtual void CopyTransactionWrites(TPersistedStateTransactionBase& srcTx, TPersistedStateTransactionBase& dstTx) = 0;
    //! Convert type dependent record to type independet row.
    virtual TStorageRow RecordToRow(const TPersistedStateRecordBase& record) const = 0;
    //! Set read-only/read-write mode.
    virtual void SetMode(bool isReadOnly) = 0;
    //! Leave the control.
    virtual void LeaveControl() = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Connection between SequenceId, the state that owns a record with that ID and the record itself.
template <class TDBKey, class TDBValue>
struct TPersistedStateKnownRecord
{
    TWeakPtr<IPersistedStateImpl<TDBKey, TDBValue>> State;
    TPersistedStateRecordBasePtr Record;
};

//! Comparator for TPersistedStateKnownRecord. Allows to compare directly with TSequenceId.
template <class TDBKey, class TDBValue>
struct TPersistedStateKnownRecordLess
{
    using TKnownRecord = TPersistedStateKnownRecord<TDBKey, TDBValue>;
    // Enable heterogeneous lookup in std::set, so one can pass directly TSequenceId to `find`, `lower_bound`, etc.
    using is_transparent = void;

    constexpr bool operator()(const TKnownRecord& lhs, const TKnownRecord& rhs) const
    {
        return lhs.Record->SequenceId < rhs.Record->SequenceId;
    }

    constexpr bool operator()(const TKnownRecord& lhs, const TSequenceId& rhs) const
    {
        return lhs.Record->SequenceId < rhs;
    }

    constexpr bool operator()(const TSequenceId& lhs, const TKnownRecord& rhs) const
    {
        return lhs < rhs.Record->SequenceId;
    }
};

////////////////////////////////////////////////////////////////////////////////

//! A structure that describes one point in lifetime of state.
struct TPersistedStateCheckpoint
    : public TRefCounted
    , public TIntrusiveListItem<TPersistedStateCheckpoint>
{
    //! Sequence ID of control state when the checkpoint was created. All checkpoints must have different sequence IDs.
    TSequenceId SequenceId;
    //! Readers of the state snapshot with SequenceId.
    TIntrusiveList<TPersistedStateTransactionBase> Readers;
    //! Writer that changes the state. Every checkpoint in a list except the last element has it's writer.
    TPersistedStateTransactionPtr Writer;
};

DEFINE_REFCOUNTED_TYPE(TPersistedStateCheckpoint);

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
class TPersistedStateImpl
    : public IPersistedStateImpl<TDBKey, TDBValue>
    , public TPersistedState<TKey, TValue>
{
protected:
    using TRecord = typename TPersistedState<TKey, TValue>::TRecord;
    using TRecordPtr = typename TPersistedState<TKey, TValue>::TRecordPtr;
    using TKnownRecord = TPersistedStateKnownRecord<TDBKey, TDBValue>;
    using TStorageRow = TPersistedStateStorageRow<TDBKey, TDBValue>;

    friend class TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>;
    using TControlWeakPtr = TWeakPtr<TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>>;

    using TPersistedState<TKey, TValue>::IsReadOnly_;
    using TPersistedState<TKey, TValue>::IsOrphan_;
    using TPersistedState<TKey, TValue>::RecordSet_;

private:
    const TControlWeakPtr WeakControl_;
    const TPersistedStateName Name_;
    TSerializer Serializer_;

public:
    TPersistedStateImpl(TControlWeakPtr weakControl, const TPersistedStateName& name, const TSerializer& serializer = {})
        : WeakControl_(std::move(weakControl))
        , Name_(name)
        , Serializer_(serializer)
    { }

    //! Redirect row to TPersistedState::Apply().
    void Apply(
        TSequenceId sequenceId,
        const TDBKey& keyLeft,
        const TDBKey& keyRight,
        const TDBValue& value,
        std::deque<TSequenceId>* droppedIds) override;
    //! Redirect row to TPersistedState::CheckContents.
    void CheckContents(const TPersistedStateName& name) const override;

    //! StartTransaction transaction.
    TPersistedStateTransactionPtr StartTransaction() override;
    //! Check that the transaction has no conflicts with earlier committed transactions.
    void ValidateTransaction(TPersistedStateTransactionBase& tx, TPersistedStateTransactionContextBase& context) override;
    //! Redirect to TPersistedState::PrepareTransaction.
    void PrepareTransaction(
        TPersistedStateTransactionContextBase& context,
        std::vector<TKnownRecord>& introduceRecords,
        std::vector<TStorageRow>& insertRows,
        std::vector<TSequenceId>& deleteRecords) override;
    //! Redirect to TPersistedState::RevertTransaction.
    void RevertTransaction(TPersistedStateTransactionContextBase& context) override;
    //! Redirect to TPersistedState::CompleteTransaction.
    void CompleteTransaction(TPersistedStateTransactionContextBase& context) override;

    //! Convert type dependent record to type independet row.
    TStorageRow RecordToRow(const TPersistedStateRecordBase& record) const override;
    //! Set read-only/read-write mode.
    void SetMode(bool isReadOnly) override;
    //! Leave the control.
    void LeaveControl() override;
    //! Redirect to TPersistedStateControl::GetSequenceId().
    TSequenceId GetControlSequenceId() const override;
    //! Construct known records and pass them to TPersistedStateControl::Notify().
    void NotifyControl(
        std::vector<TRecordPtr>&& insertRecords,
        const std::vector<TSequenceId>& deleteRecords) override;
    //! Clone the object.
    TPersistedStatePtr<TKey, TValue> Clone(TPersistedStateControlBase& control) const override;
    //! Retrieve transaction context base.
    TPersistedStateTransactionContextBase& GetTransactionContextBase(TPersistedStateTransactionPtr& tx) override;
    //! Retrieve transactional lock.
    TGuard<NThreading::TSpinLock> TransactionGuard() const override;
    //! Create new transaction context base.
    TPersistedStateTransactionContextPtr CreateTransactionContextBase(TPersistedStateTransactionBase& tx, bool readOnly) override;
    //! Copy this state's write set from srcTx's context into dstTx's context.
    void CopyTransactionWrites(TPersistedStateTransactionBase& srcTx, TPersistedStateTransactionBase& dstTx) override;
};

////////////////////////////////////////////////////////////////////////////////

template <class TDBKey, class TDBValue, class TDefaultSerializer>
class TPersistedStateTransaction
    : public TPersistedStateTransactionBase
{
public:
    explicit TPersistedStateTransaction(TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>& control, bool readOnly);

    ~TPersistedStateTransaction() override;

    //! Active -> Prepared -> Committed/Reverted.
    void Commit(TPersistedStateCommitContext* context = nullptr) override;
    //! Active -> Aborted.
    void Abort() override;

private:
    template <class TKey, class TValue, class TSerializer, class TAnyDBKey, class TAnyDBValue, class TAnyDefaultSerializer>
    friend class TPersistedStateImpl;
    template <class TAnyDBKey, class TAnyDBValue, class TAnyDefaultSerializer>
    friend class TPersistedStateControl;

    using TPersistedStateImplPtr = TIntrusivePtr<IPersistedStateImpl<TDBKey, TDBValue>>;
    using TStorageRow = TPersistedStateStorageRow<TDBKey, TDBValue>;
    using TKnownRecord = TPersistedStateKnownRecord<TDBKey, TDBValue>;

    //! Pointer to control.
    const TWeakPtr<TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>> WeakControl_;
    //! For each state there's a transaction context in this map.
    THashMap<TPersistedStateName, std::pair<TPersistedStateImplPtr, TPersistedStateTransactionContextPtr>> Contexts_;
    //! Saved control's PersistFailureEpoch_ when the transaction is started.
    //! When persist fails and control increments PersistFailureEpoch_, all transactions with lower PersistFailureEpoch_
    //! are treated as conflicted and will be aborted.
    ssize_t PersistFailureEpoch_ = 0;

    //! Checkpoint in which the transaction is registered as reader.
    TWeakPtr<TPersistedStateCheckpoint> ReadCheckpoint_;
    //! Checkpoint in which the transaction is registered as writer.
    TWeakPtr<TPersistedStateCheckpoint> WriteCheckpoint_;

    //! When a transaction is prepared it introduces new known records. If reverted after, it must remove the back.
    //! For that purpose in this field the sequence IDs of the introduced known records are saved.
    std::vector<TSequenceId> UndoInserts_;
    //! When a transaction is prepared it changes LastKnownId_. If reverted after, it should restore original LastKnownId_.
    //! For that purpose in this field the original LastKnownId_ is saved.
    //! More precisely: control's LastKnownId_ before the transaction was prepared.
    std::optional<TSequenceId> UndoLastKnownId_;

    //! Records that were introduced by this transaction , collected from all states.
    std::vector<TKnownRecord> IntroduceRecords_;
    //! DB records that were introduced by this transaction, collected from all states.
    std::vector<TStorageRow> PersistInsertRows_;
    //! Sequence IDs of deleted by this transaction records, collected from all states.
    std::vector<TSequenceId> DeleteIds_;
    //! More precisely: control's LastKnownId_ after the transaction was prepared.
    TSequenceId IntroduceSequenceId_ = FakeSequenceId;

    //! User commit state, if it was passed as an argument of transaction->Commit(<here>);
    TPersistedStateCommitContext* PersistContext_ = nullptr;
    //! Synchronisation primitives for concurrent commits of transactions.
    //! If transaction persist is already in progress, a transaction must wait until its finished using the future.
    //! The future return a bool value:
    //!  true: it must proceed with persisting of itself (perhaps in batch with other waiting transactions).
    //!  false: it was already persisted in batch with some other transactions.
    TPromise<bool> PersistPromise_;
    TFuture<bool> PersistFuture_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>::Apply(
    TSequenceId sequenceId,
    const TDBKey& keyLeft,
    const TDBKey& keyRight,
    const TDBValue& value,
    std::deque<TSequenceId>* droppedIds)
{
    TPersistedState<TKey, TValue>::Apply(sequenceId,
        Serializer_.Decode(keyLeft, std::type_identity<TKey>{}),
        Serializer_.Decode(keyRight, std::type_identity<TKey>{}),
        Serializer_.Decode(value, std::type_identity<TValue>{}),
        droppedIds);
}

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>::CheckContents(const TPersistedStateName& name) const
{
    TPersistedState<TKey, TValue>::CheckContents(name);
}

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
TPersistedStateTransactionPtr TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>::StartTransaction()
{
    auto control = WeakControl_.Lock();
    THROW_ERROR_EXCEPTION_UNLESS(control, "Persisted state control has been destroyed");
    return control->StartTransaction();
}

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>::ValidateTransaction(
    TPersistedStateTransactionBase& transactionBase,
    TPersistedStateTransactionContextBase& contextBase)
{
    auto& context = static_cast<TPersistedStateTransactionContext<TKey, TValue>&>(contextBase);
    if (context.ReadSet.empty() && !context.ReadAll) {
        return;
    }

    // There are two way to find R-W conflict between current transaction and other transactions.
    // 1. Iterate over all keys written by other transactions and ensure current one has not read the keys.
    // 2. Iterate over all keys read by current transaction and ensure that actual RecordSet still hold the same value.
    // It's hard to determine which way is faster, so let's start both ways concurrently, and propagate ways by rotation;
    // if one of the check is completed, then there's guarantee that there are no conflicts.

    // Iterator for second-way check.
    auto readSetCheck = context.ReadSet.begin();

    auto controlBase = WeakControl_.Lock();
    THROW_ERROR_EXCEPTION_UNLESS(controlBase, "Persisted state control has been destroyed");
    auto& control = static_cast<TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>&>(*controlBase);
    auto& transaction = static_cast<TPersistedStateTransaction<TDBKey, TDBValue, TDefaultSerializer>&>(transactionBase);
    auto strongCheckpoint = transaction.ReadCheckpoint_.Lock();
    YT_VERIFY(strongCheckpoint);
    auto* checkpoint = strongCheckpoint.Get();
    while (true) {
        if (checkpoint->Writer) {
            auto& writerTransaction = static_cast<TPersistedStateTransaction<TDBKey, TDBValue, TDefaultSerializer>&>(*checkpoint->Writer);
            auto& [statAgain, writerContextBase] = writerTransaction.Contexts_.at(Name_);
            auto& writerContext = static_cast<TPersistedStateTransactionContext<TKey, TValue>&>(*writerContextBase);
            if (context.ReadAll) {
                if (!writerContext.WriteSet.empty()) {
                    THROW_ERROR_EXCEPTION("Transaction has been aborted due to conflict (read all while overwritten)");
                }
            } else {
                for (const auto& [key, value] : writerContext.WriteSet) {
                    // First-way check: ensure that nobody overwrote the read set.
                    if (context.ReadSet.contains(key)) {
                        THROW_ERROR_EXCEPTION("Transaction has been aborted due to conflict (first-way R-W check)");
                    }
                    if (readSetCheck != context.ReadSet.end()) {
                        // Concurrent second-way check: check that record set was not changed.
                        auto it = RecordSet_.find(readSetCheck->first);
                        auto currentVersion = it != RecordSet_.end() ? std::optional((*it)->SequenceId) : std::nullopt;
                        if (currentVersion != readSetCheck->second) {
                            THROW_ERROR_EXCEPTION("Transaction has been aborted due to conflict (second-way R-W check)");
                        }
                        ++readSetCheck;
                    } else {
                        // Second-way check completed first. There are no conflicts found.
                        context.ReadSet.clear();
                        return;
                    }
                }
            }
        }
        if (checkpoint == control.CheckpointList_.Back()) {
            break;
        }
        checkpoint = checkpoint->Next()->Node();
    }
    // First-way check completed first. There are no conflicts found.
    context.ReadSet.clear();
}

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>::PrepareTransaction(
    TPersistedStateTransactionContextBase& contextBase,
    std::vector<TKnownRecord>& introduceRecords,
    std::vector<TStorageRow>& insertRows,
    std::vector<TSequenceId>& deleteRecords)
{
    auto& context = static_cast<TPersistedStateTransactionContext<TKey, TValue>&>(contextBase);
    std::vector<TRecordPtr> insertRecords;
    TPersistedState<TKey, TValue>::PrepareTransaction(context, insertRecords, deleteRecords);
    for (auto& record : insertRecords) {
        insertRows.push_back(TStorageRow{
            record->SequenceId,
            EStorageRowFlags::None,
            Name_,
            Serializer_.Encode(record->Interval.ExtractLeft()),
            Serializer_.Encode(record->Interval.ExtractRight()),
            Serializer_.Encode(record->Value)});
        introduceRecords.push_back(TKnownRecord{MakeWeak(this), std::move(record)});
    }
}

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>::RevertTransaction(TPersistedStateTransactionContextBase& contextBase)
{
    auto& context = static_cast<TPersistedStateTransactionContext<TKey, TValue>&>(contextBase);
    TPersistedState<TKey, TValue>::RevertTransaction(context);
}

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>::CompleteTransaction(TPersistedStateTransactionContextBase& contextBase)
{
    auto& context = static_cast<TPersistedStateTransactionContext<TKey, TValue>&>(contextBase);
    TPersistedState<TKey, TValue>::CompleteTransaction(context);
}

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
TPersistedStateStorageRow<TDBKey, TDBValue> TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>::RecordToRow(
    const TPersistedStateRecordBase& recordBase) const
{
    const auto& record = static_cast<const TRecord&>(recordBase);
    return TStorageRow{
        record.SequenceId,
        EStorageRowFlags::None,
        Name_,
        Serializer_.Encode(record.Interval.ExtractLeft()),
        Serializer_.Encode(record.Interval.ExtractRight()),
        Serializer_.Encode(record.Value)};
}

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>::SetMode(bool isReadOnly)
{
    IsReadOnly_ = isReadOnly;
}

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>::LeaveControl()
{
    IsOrphan_ = true;
}

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
TSequenceId TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>::GetControlSequenceId() const
{
    auto control = WeakControl_.Lock();
    THROW_ERROR_EXCEPTION_UNLESS(control, "Persisted state control has been destroyed");

    return control->GetSequenceId();
}

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>::NotifyControl(
    std::vector<TRecordPtr>&& insertRecords,
    const std::vector<TSequenceId>& deleteRecords)
{
    auto control = WeakControl_.Lock();
    THROW_ERROR_EXCEPTION_UNLESS(control, "Persisted state control has been destroyed");

    std::vector<TKnownRecord> introduceRecords;
    introduceRecords.reserve(insertRecords.size());
    for (auto& record : insertRecords) {
        introduceRecords.emplace_back(TKnownRecord{MakeWeak(this), std::move(record)});
    }
    control->Notify(std::move(introduceRecords), deleteRecords);
}

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
TPersistedStatePtr<TKey, TValue> TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>::Clone(TPersistedStateControlBase& control) const
{
    auto& realControl = static_cast<TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>&>(control);
    THROW_ERROR_EXCEPTION_IF(realControl.States_.contains(Name_),
        "State with name '%v' already registered",
        Name_);
    auto res = New<TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>>(MakeWeak(&realControl), Name_, Serializer_);
    res->RecordSet_ = this->RecordSet_;
    res->Subscriptions_ = this->Subscriptions_;
    res->NextSubscriptionId_ = this->NextSubscriptionId_;
    res->IsReadOnly_ = this->IsReadOnly_;
    res->IsOrphan_ = this->IsOrphan_;
    realControl.States_.emplace(res->Name_, res);
    std::vector<TRecordPtr> insertRecords;
    insertRecords.reserve(res->RecordSet_.size());
    for (const auto& record : res->RecordSet_) {
        if (!record->IsFake()) {
            insertRecords.push_back(record);
        }
    }
    std::vector<TSequenceId> deleteRecords;
    res->NotifyControl(std::move(insertRecords), deleteRecords);
    return {std::move(res)};
}

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
TGuard<NThreading::TSpinLock> TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>::TransactionGuard() const
{
    auto control = WeakControl_.Lock();
    THROW_ERROR_EXCEPTION_UNLESS(control, "Persisted state control has been destroyed");
    return Guard(control->TransactionalLock_);
}

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
TPersistedStateTransactionContextBase& TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>::GetTransactionContextBase(
    TPersistedStateTransactionPtr& txnBase)
{
    auto& tx = static_cast<TPersistedStateTransaction<TDBKey, TDBValue, TDefaultSerializer>&>(*txnBase);
    THROW_ERROR_EXCEPTION_UNLESS(tx.WeakControl_ == WeakControl_, "Transaction object must be used within the same control in which it was created");
    auto& [state, context] = GetOrCrash(tx.Contexts_, Name_);
    return *context;
}

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
TPersistedStateTransactionContextPtr TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>::CreateTransactionContextBase(
    TPersistedStateTransactionBase& txnBase,
    bool readOnly)
{
    auto& tx = static_cast<TPersistedStateTransaction<TDBKey, TDBValue, TDefaultSerializer>&>(txnBase);
    THROW_ERROR_EXCEPTION_UNLESS(tx.WeakControl_ == WeakControl_, "Transaction object must be used within the same control in which it was created");
    static_assert(std::derived_from<TPersistedStateTransactionContext<TKey, TValue>, TRefCountedBase>);
    auto context = New<TPersistedStateTransactionContext<TKey, TValue>>(RecordSet_, readOnly);
    return context;
}

template <class TKey, class TValue, class TSerializer, class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>::CopyTransactionWrites(
    TPersistedStateTransactionBase& srcTxnBase,
    TPersistedStateTransactionBase& dstTxnBase)
{
    auto& srcTxn = static_cast<TPersistedStateTransaction<TDBKey, TDBValue, TDefaultSerializer>&>(srcTxnBase);
    auto& dstTxn = static_cast<TPersistedStateTransaction<TDBKey, TDBValue, TDefaultSerializer>&>(dstTxnBase);
    auto& [srcState, srcContextBase] = srcTxn.Contexts_.at(Name_);
    auto& [dstState, dstContextBase] = dstTxn.Contexts_.at(Name_);
    auto& srcContext = static_cast<TPersistedStateTransactionContext<TKey, TValue>&>(*srcContextBase);
    auto& dstContext = static_cast<TPersistedStateTransactionContext<TKey, TValue>&>(*dstContextBase);
    // The destination is a fresh read-only snapshot whose RecordSet base equals the (still uncommitted)
    // base from which the source write transaction started, so committed RecordSet + copied WriteSet
    // reproduces the source's in-flight view. Values are immutable cloned pointers, so a shallow copy is safe.
    dstContext.WriteSet = srcContext.WriteSet;
    dstContext.RecordSetSizeDiff = srcContext.RecordSetSizeDiff;
}

////////////////////////////////////////////////////////////////////////////////

template <class TDBKey, class TDBValue, class TDefaultSerializer>
TPersistedStateTransaction<TDBKey, TDBValue, TDefaultSerializer>::TPersistedStateTransaction(
    TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>& control,
    bool readOnly)
    : WeakControl_(&control)
{
    IsReadOnly_ = readOnly;
    for (auto& [name, state] : control.States_) {
        Contexts_.emplace(name, std::pair(state, state->CreateTransactionContextBase(*this, readOnly)));
    }
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
TPersistedStateTransaction<TDBKey, TDBValue, TDefaultSerializer>::~TPersistedStateTransaction()
{
    if (State_ == EPersistedStateTransactionState::Active) {
        auto control = WeakControl_.Lock();
        if (control) {
            control->AbortTransaction(*this);
        }
    }
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateTransaction<TDBKey, TDBValue, TDefaultSerializer>::Commit(TPersistedStateCommitContext* commitContext)
{
    auto control = WeakControl_.Lock();
    THROW_ERROR_EXCEPTION_UNLESS(control, "Persisted state control has been destroyed");
    control->CommitTransaction(*this, commitContext);
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::CommitTransaction(
    TTransaction& transaction,
    TPersistedStateCommitContext* commitContext)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        transaction.State_ == EPersistedStateTransactionState::Active,
        "Transaction must be active but is in %Qlv state",
        transaction.State_);

    auto guard = Guard(TransactionalLock_);

    ValidateTransaction(transaction, guard);
    UnregisterAsReader(transaction, guard);
    PrepareTransaction(transaction, commitContext);
    if (!transaction.IntroduceRecords_.empty() || !transaction.DeleteIds_.empty()) {
        RegisterAsWriter(transaction, guard);
    }
    CleanupCheckpoints(guard);

    if (!Storage_) {
        // It is allowed to commit read-only transactions on replica.
        YT_ASSERT(transaction.IntroduceRecords_.size() == 0);
        YT_ASSERT(transaction.PersistInsertRows_.size() == 0);
        YT_ASSERT(transaction.DeleteIds_.size() == 0);
        YT_ASSERT(transaction.PersistContext_ == nullptr);
        CompleteTransaction(transaction);
        return;
    }

    transaction.PersistPromise_ = NewPromise<bool>();
    transaction.PersistFuture_ = transaction.PersistPromise_.ToFuture();
    PersistPendingTransactions_.push_back(MakeStrong<TPersistedStateTransactionBase>(&transaction));
    // PersistPendingTransactions_ forms a batch which will be persisted at once. The first transaction in this batch should perform persist.
    bool mustPersistNow = !PersistInProgress_ && PersistPendingTransactions_.size() == 1;

    guard.Release();

    if (!mustPersistNow) {
        // Wait until in-progress persist is finished. If the transaction was not persisted then it must persist by itself.
        mustPersistNow = NConcurrency::WaitFor(transaction.PersistFuture_).ValueOrThrow();
    }
    if (mustPersistNow) {
        Persist();
    }

    CompleteTransaction(transaction);
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateTransaction<TDBKey, TDBValue, TDefaultSerializer>::Abort()
{
    auto control = WeakControl_.Lock();
    THROW_ERROR_EXCEPTION_UNLESS(control, "Persisted state control has been destroyed");
    control->AbortTransaction(*this);
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::AbortTransaction(TTransaction& transaction)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        transaction.State_ == EPersistedStateTransactionState::Active,
        "Transaction must be active but is in %Qlv state",
        transaction.State_);

    transaction.Contexts_.clear();
    transaction.State_ = EPersistedStateTransactionState::Aborted;

    auto guard = Guard(TransactionalLock_);
    UnregisterAsReader(transaction, guard);
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::ValidateTransaction(
    TTransaction& transaction,
    TGuard<NThreading::TSpinLock>& guard)
{
    YT_ASSERT(transaction.State_ == EPersistedStateTransactionState::Active);
    auto failureGuard = Finally([&] {
        UnregisterAsReader(transaction, guard);
        transaction.State_ = EPersistedStateTransactionState::Conflicted;
    });

    if (transaction.PersistFailureEpoch_ != PersistFailureEpoch_) {
        THROW_ERROR_EXCEPTION("Transaction has been aborted due to conflict (epoch changed)");
    }
    if (transaction.HasNoWrites_) {
        // Read-only transactions with fully functional snapshot view are definitely correct and serializable.
        failureGuard.Release();
        return;
    }
    for (auto& [name, stateAndContext] : transaction.Contexts_) {
        auto& [state, context] = stateAndContext;
        state->ValidateTransaction(transaction, *context);
    }

    failureGuard.Release();
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::PrepareTransaction(
    TTransaction& transaction,
    TPersistedStateCommitContext* commitContext)
{
    YT_ASSERT(transaction.State_ == EPersistedStateTransactionState::Active);
    transaction.State_ = EPersistedStateTransactionState::Prepared;

    auto failureGuard = Finally([&] {
        RevertTransaction(transaction);
    });

    for (auto& [name, stateAndContext] : transaction.Contexts_) {
        auto& [state, context] = stateAndContext;
        state->PrepareTransaction(*context, transaction.IntroduceRecords_, transaction.PersistInsertRows_, transaction.DeleteIds_);
    }
    YT_ASSERT(std::ssize(transaction.IntroduceRecords_) == std::ssize(transaction.PersistInsertRows_));

    transaction.UndoLastKnownId_ = LastKnownId_;
    auto genId = LastKnownId_.Underlying();
    for (ssize_t i = 0; i < std::ssize(transaction.IntroduceRecords_); i++) {
        auto id = ++genId;
        transaction.IntroduceRecords_[i].Record->SequenceId = TSequenceId{id};
        transaction.PersistInsertRows_[i].SequenceId = TSequenceId{id};
    }
    LastKnownId_ = transaction.IntroduceSequenceId_ = TSequenceId{genId};

    transaction.UndoInserts_.reserve(transaction.IntroduceRecords_.size());
    for (auto& record : transaction.IntroduceRecords_) {
        transaction.UndoInserts_.push_back(record.Record->SequenceId);
        KnownRecords_.insert(std::move(record));
    }

    transaction.PersistContext_ = commitContext;

    failureGuard.Release();
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::RevertTransaction(TTransaction& transaction)
{
    YT_ASSERT(transaction.State_ == EPersistedStateTransactionState::Prepared);
    transaction.State_ = EPersistedStateTransactionState::Reverted;

    for (auto& [name, stateAndContext] : transaction.Contexts_) {
        auto& [state, context] = stateAndContext;
        state->RevertTransaction(*context);
    }

    for (auto sequenceId : transaction.UndoInserts_) {
        KnownRecords_.erase(KnownRecords_.find(sequenceId));
    }
    transaction.UndoInserts_.clear();
    if (transaction.UndoLastKnownId_) {
        LastKnownId_ = *transaction.UndoLastKnownId_;
        transaction.UndoLastKnownId_ = std::nullopt;
    }
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::CompleteTransaction(TTransaction& transaction)
{
    YT_ASSERT(transaction.State_ == EPersistedStateTransactionState::Prepared);
    transaction.State_ = EPersistedStateTransactionState::Committed;

    for (auto& [name, stateAndContext] : transaction.Contexts_) {
        auto& [state, context] = stateAndContext;
        state->CompleteTransaction(*context);
    }
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
TSequenceId TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::GetSequenceId() const
{
    auto guard = Guard(TransactionalLock_);
    return LastKnownId_;
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::Notify(
    std::vector<TKnownRecord>&& introduceRecords,
    const std::vector<TSequenceId>& deleteRecords)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        std::ranges::all_of(introduceRecords, [] (const auto& record) {
            return record.Record->SequenceId != FakeSequenceId;
        }),
        "Bad database: introducing fake sequence ID");
    YT_ASSERT(std::ranges::all_of(deleteRecords, [] (const auto sequenceId) {
        return sequenceId != FakeSequenceId;
    }));

    for (auto sequenceId : deleteRecords) {
        KnownRecords_.erase(KnownRecords_.find(sequenceId));
    }
    for (auto& record : introduceRecords) {
        KnownRecords_.insert(std::move(record));
    }
    LastKnownId_ = KnownRecords_.empty() ? FakeSequenceId : KnownRecords_.rbegin()->Record->SequenceId;
    ConfirmedId_ = LastKnownId_;
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::Persist()
{
    YT_ASSERT(Storage_);

    std::vector<TPersistedStateTransactionPtr> transactions;
    {
        auto guard = Guard(TransactionalLock_);
        transactions.swap(PersistPendingTransactions_);
        PersistInProgress_ = true;
    }

    auto failureGuard = Finally([&] {
        auto guard = Guard(TransactionalLock_);
        for (auto& transactionBasePtr : PersistPendingTransactions_) {
            transactions.push_back(std::move(transactionBasePtr));
        }
        PersistPendingTransactions_.clear();
        for (ssize_t i = std::ssize(transactions) - 1; i >= 0; i--) {
            auto& transaction = static_cast<TTransaction&>(*transactions[i]);
            RevertTransaction(transaction);
            if (i > 0) {
                transaction.PersistPromise_.Set(TError("Operation over state was failed to persist"));
            }
        }
        PersistInProgress_ = false;
        PersistFailureEpoch_++;
        CheckpointList_.Clear();
        CheckpointDeque_.clear();
    });

    ssize_t executeLimit = Storage_->GetMaxExecuteSize();
    std::vector<TStorageRow> insertRows;
    std::vector<TSequenceId> deleteRecords;
    std::vector<TPersistedStateCommitContext*> commitContexts;
    for (const auto& transactionBasePtr : transactions) {
        auto& transaction = static_cast<TTransaction&>(*transactionBasePtr);
        if (transaction.PersistContext_ != nullptr) {
            executeLimit -= transaction.PersistContext_->StatementCount;
            commitContexts.push_back(std::move(transaction.PersistContext_));
        }
        insertRows.insert(insertRows.end(), transaction.PersistInsertRows_.begin(), transaction.PersistInsertRows_.end());
        deleteRecords.insert(deleteRecords.end(), transaction.DeleteIds_.begin(), transaction.DeleteIds_.end());
    }
    THROW_ERROR_EXCEPTION_UNLESS(executeLimit > 0, "Conflicting MaxExecuteSize and StatementCount");

    if (!insertRows.empty()) {
        insertRows.back().Flags |= EStorageRowFlags::Commit;
    }

    std::vector<TSequenceId> deletePart;
    // First of all it is necessary to clean up all the garbage.
    while (!GarbageIds_.empty()) {
        ssize_t curSize = std::min(executeLimit, std::ssize(GarbageIds_));
        deletePart.resize(curSize);
        for (ssize_t i = 0; i < curSize; i++) {
            deletePart[i] = GarbageIds_[i];
        }
        Storage_->Execute({}, deletePart, /*isFinal*/ false, commitContexts);
        while (curSize-- > 0) {
            GarbageIds_.pop_front();
        }
    }

    ssize_t insertOffset = 0;
    ssize_t deleteOffset = 0;
    std::vector<TStorageRow> insertPart;
    bool metFinal = false;
    do {
        ssize_t currentInsertCount = std::min(std::ssize(insertRows) - insertOffset, executeLimit);
        ssize_t currentDeleteCount = currentInsertCount != 0 ? 0 : std::min(std::ssize(deleteRecords) - deleteOffset, executeLimit);
        bool isFinal = insertOffset + currentInsertCount == std::ssize(insertRows) && !metFinal;
        metFinal = metFinal || isFinal;

        insertPart.clear();
        insertPart.resize(currentInsertCount);
        for (ssize_t i = 0; i < currentInsertCount; i++) {
            // Push ID to garbage list in order to rollback DB changes in future if an error occurs now.
            // Push in reverse order to drop final record (if any) first of all.
            GarbageIds_.push_front(insertRows[i + insertOffset].SequenceId);
            insertPart[i] = std::move(insertRows[i + insertOffset]);
        }
        deletePart.resize(currentDeleteCount);
        for (ssize_t i = 0; i < currentDeleteCount; i++) {
            deletePart[i] = deleteRecords[i + deleteOffset];
        }

        if (isFinal) {
            Storage_->Execute(std::move(insertPart), deletePart, isFinal, commitContexts);
            // Final transaction is successfully committed, now the DB has correct state.
            GarbageIds_.clear();
        } else if (!metFinal) {
            Storage_->Execute(std::move(insertPart), deletePart, isFinal, commitContexts);
        } else {
            // The final transaction has been committed before. The only thing to do is to delete garbage.
            try {
                Storage_->Execute(std::move(insertPart), deletePart, isFinal, commitContexts);
            } catch (const std::exception&) {
                // Not a big deal, we just left some garbage in DB. Just save it to delete in future.
                for (ssize_t i = deleteOffset; i < std::ssize(deleteRecords); i++) {
                    GarbageIds_.push_back(deleteRecords[i]);
                }
                break;
            }
        }

        insertOffset += currentInsertCount;
        deleteOffset += currentDeleteCount;
    } while (insertOffset + deleteOffset < std::ssize(insertRows) + std::ssize(deleteRecords));

    {
        auto guard = Guard(TransactionalLock_);
        for (const auto& transactionBasePtr : transactions) {
            auto& transaction = static_cast<TTransaction&>(*transactionBasePtr);
            for (auto sequenceId : transaction.DeleteIds_) {
                KnownRecords_.erase(KnownRecords_.find(sequenceId));
            }
        }
        auto& lastTransaction = static_cast<TTransaction&>(*transactions.back());
        ConfirmedId_ = lastTransaction.IntroduceSequenceId_;
        PersistInProgress_ = false;
        if (!PersistPendingTransactions_.empty()) {
            auto& transaction = static_cast<TTransaction&>(*PersistPendingTransactions_.front());
            transaction.PersistPromise_.Set(true);
        }
    }

    for (ssize_t i = 1; i < std::ssize(transactions); i++) {
        auto& transaction = static_cast<TTransaction&>(*transactions[i]);
        transaction.PersistPromise_.Set(false);
    }

    failureGuard.Release();
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::TPersistedStateControl(TStorageHandlerPtr storage)
    : Storage_(std::move(storage))
{ }

template <class TDBKey, class TDBValue, class TDefaultSerializer>
TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::~TPersistedStateControl()
{
    for (auto& [name, state] : States_) {
        state->LeaveControl();
    }
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
TIntrusivePtr<TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>> TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::Clone()
{
    auto result = New<TPersistedStateControl>(Storage_);
    result->LastKnownId_ = LastKnownId_;
    result->GarbageIds_ = GarbageIds_;
    return result;
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
template <class TKey, class TValue, class TSerializer>
TPersistedStatePtr<TKey, TValue> TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::CreateState(
    const TPersistedStateName& name,
    const TSerializer& serializer)
{
    THROW_ERROR_EXCEPTION_IF(IsRecovered_, "States must be created before recover");
    THROW_ERROR_EXCEPTION_IF(States_.contains(name),
        "State with name '%v' already registered",
        name);
    static_assert(std::derived_from<TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>, TRefCountedBase>);
    auto ptr = New<TPersistedStateImpl<TKey, TValue, TSerializer, TDBKey, TDBValue, TDefaultSerializer>>(MakeWeak(this), name, serializer);
    States_.emplace(name, ptr);
    return ptr;
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
template <class TKey, class TValue>
TPersistedStatePtr<TKey, TValue> TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::CloneState(const TPersistedStatePtr<TKey, TValue>& state)
{
    return state->Clone(*this);
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::Recover()
{
    THROW_ERROR_EXCEPTION_IF(IsRecovered_, "Can't recover twice");
    THROW_ERROR_EXCEPTION_UNLESS(Storage_, "Can't recover because is not leader");
    YT_ASSERT(GarbageIds_.empty());
    std::vector<TStorageRow> rows;
    TSequenceId lastId = LastKnownId_;
    while (true) {
        ssize_t oldSize = std::ssize(rows);
        Storage_->Select(lastId, rows);
        ssize_t newSize = std::ssize(rows);
        if (!rows.empty()) {
            lastId = rows.back().SequenceId;
        }
        if (newSize - oldSize < Storage_->GetMaxSelectSize()) {
            break;
        }
    }
    while (!rows.empty()) {
        if ((rows.back().Flags & EStorageRowFlags::Commit) != EStorageRowFlags::None) {
            break;
        }
        GarbageIds_.push_front(rows.back().SequenceId);
        rows.pop_back();
    }
    Apply(rows, &GarbageIds_);
    for (auto& [name, state] : States_) {
        state->CheckContents(name);
    }
    IsRecovered_ = true;
    for (auto& [name, state] : States_) {
        state->SetMode(/*isReadOnly*/ false);
    }
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::Apply(const TStorageRow& row, std::deque<TSequenceId>* droppedIds)
{
    auto it = States_.find(row.Name);
    if (it != States_.end()) {
        it->second->Apply(row.SequenceId, row.KeyLeft, row.KeyRight, row.Value, droppedIds);
    } else {
        YT_TLOG_EVENT_FLUENT(PersistedStateLogger, NLogging::ELogLevel::Warning, "State row not applied: state name is unknown")
            .With("StateName", row.Name);
    }
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::Apply(const std::vector<TStorageRow>& rows, std::deque<TSequenceId>* droppedIds)
{
    auto guard = Guard(TransactionalLock_);
    for (const auto& row : rows) {
        Apply(row, droppedIds);
    }
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
std::vector<TPersistedStateStorageRow<TDBKey, TDBValue>> TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::Follow(
    TSequenceId sequenceIdFrom,
    ssize_t limit)
{
    YT_TLOG_EVENT_FLUENT(PersistedStateLogger, NLogging::ELogLevel::Info, "Persisted state follow begin")
        .With("SequenceIdFrom", sequenceIdFrom);

    auto guard = Guard(TransactionalLock_);
    if (sequenceIdFrom == ConfirmedId_) {
        return {};
    }
    std::vector<TKnownRecord> foundRecords;
    for (auto it = KnownRecords_.upper_bound(sequenceIdFrom); it != KnownRecords_.end() && limit > 0; ++it, --limit) {
        if (it->Record->SequenceId > ConfirmedId_) {
            // The record is not persisted yet.
            break;
        }
        foundRecords.push_back(*it);
    }
    guard.Release();

    YT_TLOG_EVENT_FLUENT(PersistedStateLogger, NLogging::ELogLevel::Info, "Persisted state follow found")
        .With("SequenceIdFrom", sequenceIdFrom)
        .With("FoundRecords", std::ssize(foundRecords));

    std::vector<TStorageRow> result;
    for (const auto& knownRecord : foundRecords) {
        auto state = knownRecord.State.Lock();
        THROW_ERROR_EXCEPTION_IF(state == nullptr, "Persisted state was destroyed while following");
        result.emplace_back(state->RecordToRow(*knownRecord.Record));
    }

    YT_TLOG_EVENT_FLUENT(PersistedStateLogger, NLogging::ELogLevel::Info, "Persisted state follow converted")
        .With("SequenceIdFrom", sequenceIdFrom)
        .With("FoundRecords", std::ssize(result));

    return result;
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
TPersistedStateTransactionPtr TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::StartTransaction(bool readOnly, const TPersistedStateTransactionPtr& copyWritesFrom)
{
    static_assert(std::derived_from<TPersistedStateTransaction<TDBKey, TDBValue, TDefaultSerializer>, TRefCountedBase>);
    auto guard = Guard(TransactionalLock_);
    auto transaction = New<TPersistedStateTransaction<TDBKey, TDBValue, TDefaultSerializer>>(*this, readOnly);
    transaction->PersistFailureEpoch_ = PersistFailureEpoch_;
    if (copyWritesFrom) {
        // Seed the freshly created (empty) contexts with a copy of the source transaction's write set,
        // so this snapshot observes the source's still-uncommitted changes on top of committed RecordSet.
        for (auto& [name, state] : States_) {
            state->CopyTransactionWrites(*copyWritesFrom, *transaction);
        }
    }
    RegisterAsReader(*transaction, guard);
    return transaction;
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::RegisterAsReader(TTransaction& transaction, TGuard<NThreading::TSpinLock>& guard)
{
    YT_ASSERT(guard);
    // The reader transaction will have the latest read view which is described by LastKnownId_.
    TSequenceId checkpointSequenceId = LastKnownId_;
    if (CheckpointDeque_.empty() || CheckpointDeque_.back()->SequenceId != checkpointSequenceId) {
        auto checkpoint = New<TPersistedStateCheckpoint>();
        checkpoint->SequenceId = LastKnownId_;
        CheckpointList_.PushBack(checkpoint.Get());
        CheckpointDeque_.push_back(std::move(checkpoint));
    }
    CheckpointDeque_.back()->Readers.PushBack(&transaction);
    transaction.ReadCheckpoint_ = MakeWeak(CheckpointDeque_.back());
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::UnregisterAsReader(TTransaction& transaction, TGuard<NThreading::TSpinLock>& guard)
{
    YT_ASSERT(guard);
    transaction.Unlink();
    transaction.ReadCheckpoint_ = nullptr;
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::RegisterAsWriter(TTransaction& transaction, TGuard<NThreading::TSpinLock>& guard)
{
    YT_ASSERT(guard);
    // The writer must change the last checkpoint with LastKnownId_. But by design the transaction is registered as writer
    // after is was prepared, so it already introduced new known records and changed LastKnownId_.
    // Fortunately the correct LastKnownId_ is saved in transaction.UndoLastKnownId_.
    YT_ASSERT(transaction.UndoLastKnownId_.has_value());
    TSequenceId checkpointSequenceId = transaction.UndoLastKnownId_.value();
    if (CheckpointDeque_.empty() || CheckpointDeque_.back()->SequenceId != checkpointSequenceId) {
        auto checkpoint = New<TPersistedStateCheckpoint>();
        checkpoint->SequenceId = transaction.UndoLastKnownId_.value();
        CheckpointList_.PushBack(checkpoint.Get());
        CheckpointDeque_.push_back(std::move(checkpoint));
    }
    YT_ASSERT(!CheckpointDeque_.back()->Writer);
    CheckpointDeque_.back()->Writer = MakeStrong(&transaction);
    transaction.WriteCheckpoint_ = MakeWeak(CheckpointDeque_.back());
}

template <class TDBKey, class TDBValue, class TDefaultSerializer>
void TPersistedStateControl<TDBKey, TDBValue, TDefaultSerializer>::CleanupCheckpoints(TGuard<NThreading::TSpinLock>& guard)
{
    YT_ASSERT(guard);
    while (!CheckpointDeque_.empty() && CheckpointDeque_.front()->Readers.Empty()) {
        CheckpointList_.PopFront();
        CheckpointDeque_.pop_front();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
