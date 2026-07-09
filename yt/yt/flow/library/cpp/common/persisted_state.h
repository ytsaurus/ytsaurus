#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/misc/cow_tree.h>

#include "interval.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Sequence ID of the fake record (see TPersistedStateRecord). All real sequence ID will be greater than the value.
constexpr TSequenceId FakeSequenceId{0};

////////////////////////////////////////////////////////////////////////////////

//! Common (non-templated) base of TPersistedStateControl template.
struct TPersistedStateControlBase;

////////////////////////////////////////////////////////////////////////////////

//! Base of an object that can be passed by pointer to Commit(..) and that forwarded to Execute(..) of storage.
struct TPersistedStateCommitContext
{
    virtual ~TPersistedStateCommitContext() = default;

    //! If the context is used to append some statements to the same transaction, then the member should reveal the number of the statements.
    ssize_t StatementCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Common (non-templated) base of TPersistedStateRecord template.
struct TPersistedStateRecordBase
    : public TRefCounted
{
    //! Monotonically growing unique per control ID of the record.
    //! Can be FakeSequenceId for fake record, which represents an empty interval from -inf to +inf.
    //! It is needed for simplicity of code and logic: it makes any correct state to be full - covering key axis from -inf to +inf.
    //! The one and only fake record is created in constructor of TPersistedState.
    //! It should not be stored in database or even be known outside of TPersistedState.
    TSequenceId SequenceId;

    TPersistedStateRecordBase(TSequenceId sequenceId)
        : SequenceId(sequenceId)
    { }
};

using TPersistedStateRecordBasePtr = TIntrusivePtr<TPersistedStateRecordBase>;

//! Unit of storage.
template <class TKey, class TValue>
struct TPersistedStateRecord
    : TPersistedStateRecordBase
{
    //! Key interval. Can be degenerate (point) in records with value or open interval in tombstone records (without value).
    TInterval<TKey> Interval;
    //! Optional value. Has value in degenerate (point) interval and has no value in tombstone records (without value).
    std::optional<TValue> Value;

    TPersistedStateRecord(TSequenceId sequenceId, TInterval<TKey>&& interval, std::optional<TValue>&& value)
        : TPersistedStateRecordBase(sequenceId)
        , Interval(std::move(interval))
        , Value(std::move(value))
    { }

    TPersistedStateRecord(TSequenceId sequenceId, TInterval<TKey>&& interval, const std::optional<TValue>& value)
        : TPersistedStateRecordBase(sequenceId)
        , Interval(std::move(interval))
        , Value(value)
    { }

    //! Check whether the record is fake - see SequenceId member.
    bool IsFake() const
    {
        return SequenceId == FakeSequenceId;
    }
};

template <class TKey, class TValue>
using TPersistedStateRecordPtr = TIntrusivePtr<TPersistedStateRecord<TKey, TValue>>;

//! Transparent comparator for TRecord. Needed to make std::set able to use TKey as key.
template <class TKey, class TValue>
struct TPersistedStateRecordLess
{
    using is_transparent = void;
    using TRecord = TPersistedStateRecordPtr<TKey, TValue>;
    constexpr bool operator()(const TRecord& lhs, const TRecord& rhs) const;
    constexpr bool operator()(const TRecord& lhs, const TEndpoint<TKey>& rhs) const;
    constexpr bool operator()(const TEndpoint<TKey>& lhs, const TRecord& rhs) const;
    constexpr bool operator()(const TRecord& lhs, const TKey& rhs) const;
    constexpr bool operator()(const TKey& lhs, const TRecord& rhs) const;
};

//! General set of records.
template <class TKey, class TValue>
using TPersistedStateRecordSet = TCowSet<TPersistedStateRecordPtr<TKey, TValue>, TPersistedStateRecordLess<TKey, TValue>>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPersistedStateTransactionState,
    //! Newly created transaction, allowed to do anything.
    (Active)
    //! The transaction was aborted due to conflict.
    (Conflicted)
    //! Short state of a transaction when Commit() was started but not finished yet.
    (Prepared)
    //! The transaction was successfully committed.
    (Committed)
    //! The transaction has started Commit(), but was reverted after failure during persist in storage.
    (Reverted)
    //! The transaction was explicitly aborted by Abort().
    (Aborted)
);

class TPersistedStateTransactionBase
    : public TRefCounted
    , public TIntrusiveListItem<TPersistedStateTransactionBase>
{
protected:
    template <class TKey, class TValue>
    friend class TPersistedState;

    EPersistedStateTransactionState State_ = EPersistedStateTransactionState::Active;
    //! Read only status that was specified upon transaction creation.
    //! Read only transaction is cheaper and cannot fail due to conflict.
    bool IsReadOnly_ = false;
    //! Is true until any modification is made by the transaction.
    bool HasNoWrites_ = true;

public:
    //! Active -> Prepared -> Committed/Reverted.
    virtual void Commit(TPersistedStateCommitContext* context = nullptr) = 0;
    //! Active -> Aborted.
    virtual void Abort() = 0;
};

DEFINE_REFCOUNTED_TYPE(TPersistedStateTransactionBase);

class TPersistedStateTransactionContextBase
    : public TRefCounted
{ };

template <class TKey, class TValue>
class TPersistedStateTransactionContext
    : public TPersistedStateTransactionContextBase
{
public:
    const TPersistedStateRecordSet<TKey, TValue> RecordSet;
    THashMap<TKey, std::optional<TSequenceId>> ReadSet;
    bool ReadAll;
    THashMap<TKey, std::optional<TValue>> WriteSet;
    std::vector<typename TPersistedStateRecordSet<TKey, TValue>::node_type> UndoDeleted;
    //! Difference of size of visible record set size comparing to common record set size.
    size_t RecordSetSizeDiff = 0;

    TPersistedStateTransactionContext(TPersistedStateRecordSet<TKey, TValue>& recordSet, bool readOnly);
};

using TPersistedStateTransactionContextPtr = TIntrusivePtr<TPersistedStateTransactionContextBase>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPersistedStateLookupStatus,
    (WriteSetInserted)
    (WriteSetErased)
    (RecordSetFound)
    (RecordSetNotFound)
);

//! One part of state that can be represented as map or TKey->TValue pair.
template <class TKey, class TValue>
class TPersistedState
    : public virtual TRefCounted
{
public:
    using key_type = TKey;
    using mapped_type = TValue;
    using value_type = std::pair<const TKey, const TValue>;
    //! Types for Subscribe().
    YT_DEFINE_STRONG_TYPEDEF(TSubscribeCookie, int);
    YT_DEFINE_STRONG_TYPEDEF(TOnInsert, std::function<void(const TKey& key)>);
    YT_DEFINE_STRONG_TYPEDEF(TOnErase, std::function<void(const TKey& key)>);

private:
    using TRecordSet = TPersistedStateRecordSet<TKey, TValue>;
    using TRecordSetIterator = TRecordSet::iterator;
    using TWriteSet = THashMap<TKey, std::optional<TValue>>;
    using TWriteSetIterator = TWriteSet::iterator;
    using TOptKey = std::optional<TKey>;
    using TOptValue = std::optional<TValue>;
    using ELookupStatus = EPersistedStateLookupStatus;

    struct TSubscription
    {
        TOnInsert OnInsert;
        TOnErase OnErase;
    };
    template <class TAnyKey, class TAnyValue>
    friend class TPersistedStateTransactional;

public:
    //! Basic iterator for compatibility with STL.
    class TIterator : public TRecordSet::const_iterator
    {
    public:
        TIterator(TRecordSet::const_iterator it, TRecordSet::const_iterator end);

        const std::pair<const TKey, const TValue>& operator*() const;
        const std::pair<const TKey, const TValue>* operator->() const;
        TIterator& operator++();
        TIterator operator++(int);

    private:
        //! Saved end.
        TRecordSet::const_iterator End_;
        //! Personal copy of data to make dereference thread-safe.
        std::optional<std::pair<const TKey, const TValue>> Storage_;
    };

    using iterator = TIterator;

    //! More complex iterator that considers active transaction.
    class TTransactionalIterator
    {
    private:
        using TCommon = TRecordSet;
        using TCommonIterator = TCommon::const_iterator;
        using TWriteSetIterator = TWriteSet::const_iterator;

        const TCommon& Common_;
        TWriteSet& WriteSet_;
        TCommonIterator CommonIterator_;
        TWriteSetIterator WriteSetIterator_;

    public:
        TTransactionalIterator(const TCommon& common, TWriteSet& writeSet);
        TTransactionalIterator(const TCommon& common, TWriteSet& writeSet, TWriteSetIterator writeSetIterator);
        TTransactionalIterator(const TCommon& common, TWriteSet& writeSet, TCommonIterator commonIterator, TWriteSetIterator writeSetIterator, ELookupStatus status);

        struct TMemberAccess : public std::pair<const TKey&, const TValue&>
        {
            using std::pair<const TKey&, const TValue&>::pair;
            const std::pair<const TKey&, const TValue&>* operator->() const;
        };

        std::pair<const TKey&, const TValue&> operator*() const;
        TMemberAccess operator->() const;
        TTransactionalIterator& operator++();
        TTransactionalIterator operator++(int);
        bool operator==(const TTransactionalIterator& that) const;
        bool operator==(const TIterator& that) const;
    };

protected:
    using TRecord = TPersistedStateRecord<TKey, TValue>;
    using TRecordPtr = TPersistedStateRecordPtr<TKey, TValue>;

    //! Main in-memory storage. Consist of intervals with values (in degenerate intervals) or without.
    //! Normally covers entire key axis from -inf to +inf.
    TRecordSet RecordSet_;

    //! List of subscriptions by their cookie.
    THashMap<TSubscribeCookie, TSubscription> Subscriptions_;

    //! Subscription ID generator.
    int NextSubscriptionId_ = 0;

    //! Read-only state, is true until recovery is complete. Always true for replica state.
    bool IsReadOnly_ = true;

    //! Is set to true when owning control is destroyed.
    bool IsOrphan_ = false;

    //! Empty fake write set that is used when there's no active transaction.
    THashMap<TKey, std::optional<TValue>> FakeWriteSet_;

public:
    TPersistedState();

    //! Redirect to control->GetSequenceId();
    virtual TSequenceId GetControlSequenceId() const = 0;

    //! Read.
    bool contains(const TKey& key) const;
    TValue at(const TKey& key) const;
    size_t size() const;
    // Non-transactional FindPtr is not thread-safe.
    const TValue* FindPtr(const TKey& key) const;
    // `find` is thread-safe in terms of comparing with `end` and dereference, but iteration is not thread-safe.
    iterator find(const TKey& key) const;
    // Non-transactional iteration is not thread-safe.
    iterator begin() const;
    iterator end() const;

    //! Modify.
    template <std::convertible_to<TKey> TKeyRef, std::convertible_to<TValue> TValueRef>
    std::pair<iterator, bool> insert_or_assign(TKeyRef&& key, TValueRef&& value);
    template <class... T>
    std::pair<iterator, bool> emplace(T&&... t);
    size_t erase(const TKey& key);

    //! Start transaction (actually over all states, not this particular).
    virtual TPersistedStateTransactionPtr StartTransaction() = 0;

    //! Transactional read.
    bool contains(TPersistedStateTransactionPtr& tx, const TKey& key);
    TValue at(TPersistedStateTransactionPtr& tx, const TKey& key);
    size_t size(TPersistedStateTransactionPtr& tx);
    // Note that if tx == nullptr, then FindPtr is not thread-safe.
    const TValue* FindPtr(TPersistedStateTransactionPtr& tx, const TKey& key);
    // Note that if tx == nullptr, then iteration is not thread-safe.
    TTransactionalIterator find(TPersistedStateTransactionPtr& tx, const TKey& key);
    TTransactionalIterator begin(TPersistedStateTransactionPtr& tx);
    TTransactionalIterator end(TPersistedStateTransactionPtr& tx);

    //! Write down in transactional engine that the key was read by the transaction.
    void TrackRead(TPersistedStateTransactionPtr& tx, const TKey& key);
    //! Write down in transactional engine that the entire state table was read by the transaction.
    void TrackRead(TPersistedStateTransactionPtr& tx);

    //! Transactional modify.
    template <std::convertible_to<TKey> TKeyRef, std::convertible_to<TValue> TValueRef>
    std::pair<TTransactionalIterator, bool> insert_or_assign(TPersistedStateTransactionPtr& tx, TKeyRef&& key, TValueRef&& value);
    template <class... T>
    std::pair<TTransactionalIterator, bool> emplace(TPersistedStateTransactionPtr& tx, T&&... t);
    size_t erase(TPersistedStateTransactionPtr& tx, const TKey& key);

    //! Subscribe on any change, unique ID of the subscription is returned.
    TSubscribeCookie Subscribe(TOnInsert onInsert, TOnErase onErase);
    //! Unsubscribe by given subscription ID.
    void Unsubscribe(TSubscribeCookie cookie);

    //! Record count, for debug purposes only.
    ssize_t RecordCount() const;

protected:
    //! Accept as truth given record. A cleanup can be needed since the current state may be significantly outdated.
    void Apply(
        TSequenceId sequenceId,
        TOptKey&& keyLeft,
        TOptKey&& keyRight,
        TOptValue&& value,
        std::deque<TSequenceId>* droppedIds);

    //! Check that the loaded data has full coverage in terms of case. Throw an exception if there's some problem.
    //! Note that without full coverage use of persisted state is prohibited, so the contents should ne checked after recovery.
    virtual void CheckContents(const TPersistedStateName&) const;

    //! Prepare to commit transaction in this state.
    void PrepareTransaction(
        TPersistedStateTransactionContext<TKey, TValue>& context,
        std::vector<TRecordPtr>& insertRecords,
        std::vector<TSequenceId>& deleteRecords);

    //! Revert prepare.
    void RevertTransaction(TPersistedStateTransactionContext<TKey, TValue>& context);

    //! Finalize commit of a transaction in this state.
    void CompleteTransaction(TPersistedStateTransactionContext<TKey, TValue>& context);

private:
    template <class TDBKey, class TDBValue, class TDefaultSerializer>
    friend class TPersistedStateControl;

    TRecordPtr CreateRecord(TSequenceId sequenceId, TOptKey&& keyLeft, TOptKey&& keyRight, TOptValue&& value);

    TPersistedStateTransactionContext<TKey, TValue>& GetTransactionContext(TPersistedStateTransactionPtr& tx, bool forModify);

    static const char* TransactionDenyReason(const TPersistedStateTransactionPtr& tx);

    void TrackRead(TPersistedStateTransactionContext<TKey, TValue>& context, const TKey& key, std::optional<TSequenceId> readResult);
    void TrackRead(TPersistedStateTransactionContext<TKey, TValue>& context);

    //! Look for the key in write set, and if not found, in record set. If found (in any case) - return that iterator; the second will be end().
    using TLookupResult = std::tuple<TWriteSetIterator, TRecordSetIterator, ELookupStatus>;
    TLookupResult Lookup(TPersistedStateTransactionContext<TKey, TValue>& context, const TKey& key, bool trackRead);

    template <std::convertible_to<TKey> TKeyRef, std::convertible_to<std::optional<TValue>> TOptValueRef>
    void UpdateWriteSet(TPersistedStateTransactionContext<TKey, TValue>& context, TWriteSet::iterator& it, ELookupStatus lookupStatus, TKeyRef&& key, TOptValueRef&& value);

    void NotifyInsertSubscribers(const TKey& key);
    void NotifyInsertSubscribers(const TRecordPtr& keyRecord);
    void NotifyEraseSubscribers(const TKey& key);
    void NotifyEraseSubscribers(const TRecordPtr& keyRecord);

    //! Construct known records and pass them to TPersistedStateControl::Notify().
    virtual void NotifyControl(
        std::vector<TRecordPtr>&& insertRecords,
        const std::vector<TSequenceId>& deleteRecords) = 0;
    //! Clone the object.
    virtual TIntrusivePtr<TPersistedState<TKey, TValue>> Clone(TPersistedStateControlBase& control) const = 0;
    //! Retrieve transaction context base.
    virtual TPersistedStateTransactionContextBase& GetTransactionContextBase(TPersistedStateTransactionPtr& tx) = 0;
    //! Retrieve transactional lock.
    virtual TGuard<NThreading::TSpinLock> TransactionGuard() const = 0;
};

template <class TKey, class TValue>
using TPersistedStatePtr = TIntrusivePtr<TPersistedState<TKey, TValue>>;

////////////////////////////////////////////////////////////////////////////////

//! Simple wrapper around one state and one transaction that internally substitutes given transaction to state methods.
template <class TKey, class TValue>
class TTransactionalPersistedState
{
public:
    using key_type = TKey;
    using mapped_type = TValue;
    using value_type = std::pair<const TKey, const TValue>;
    using iterator = TPersistedState<TKey, TValue>::TTransactionalIterator;

private:
    TPersistedStatePtr<TKey, TValue> State_;
    TPersistedStateTransactionPtr Transaction_;

public:
    explicit TTransactionalPersistedState(TPersistedStatePtr<TKey, TValue> state = nullptr, TPersistedStateTransactionPtr transaction = nullptr);

    void SetState(TPersistedStatePtr<TKey, TValue> state);
    void SetTransaction(TPersistedStateTransactionPtr transaction = nullptr);

    //! Transactional read.
    bool contains(const TKey& key);
    TValue at(const TKey& key);
    size_t size();
    // Note that if tx == nullptr, then FindPtr is not thread-safe.
    const TValue* FindPtr(const TKey& key);
    // Note that if tx == nullptr, then iteration is not thread-safe.
    iterator find(const TKey& key);
    iterator begin();
    iterator end();

    //! Transactional modify.
    template <std::convertible_to<TKey> TKeyRef, std::convertible_to<TValue> TValueRef>
    std::pair<iterator, bool> insert_or_assign(TKeyRef&& key, TValueRef&& value);
    template <class... T>
    std::pair<iterator, bool> emplace(T&&... t);
    size_t erase(const TKey& key);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define PERSISTED_STATE_H_
#include "persisted_state-inl.h"
#undef PERSISTED_STATE_H_
