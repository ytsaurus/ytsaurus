#pragma once

#include "history_events.h"
#include "public.h"
#include "select_continuation.h"
#include "transaction_call_context.h"

#include <yt/yt/orm/client/objects/transaction_context.h>
#include <yt/yt/orm/client/objects/key.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/concurrency/async_semaphore.h>

#include <util/generic/bitmap.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

// Provides dynamic storage for commit actions.
YT_DEFINE_STRONG_TYPEDEF(TCommitActionType, int);

struct TCommitActionTypes
{
    static constexpr TCommitActionType RemoveFinalizedObjects = TCommitActionType(0);
    static constexpr TCommitActionType RemoveChildrenFinalizers = TCommitActionType(1);
    static constexpr TCommitActionType HandleRevisionUpdates = TCommitActionType(2);
    static constexpr TCommitActionType HandleAttributeMigrations = TCommitActionType(3);
};

////////////////////////////////////////////////////////////////////////////////

struct TMutatingTransactionOptions
{
    TTransactionContext TransactionContext;
    bool SkipWatchLog = false;
    bool SkipHistory = false;
    bool SkipRevisionBump = false;
    std::optional<bool> AllowRemovalWithNonemptyReferences;
};

struct TReadingTransactionOptions
{
    std::optional<bool> AllowFullScan;
};

struct TTransactionCommitResult
{
    TTimestamp CommitTimestamp = NullTimestamp;
    TInstant StartTime;
    TInstant FinishTime;
};

////////////////////////////////////////////////////////////////////////////////

struct TSetUpdateRequest
{
    NYPath::TYPath Path;
    NYTree::INodePtr Value;
    bool Recursive = false;
    std::optional<bool> SharedWrite;
    EAggregateMode AggregateMode = EAggregateMode::Unspecified;
};

struct TRemoveUpdateRequest
{
    NYPath::TYPath Path;
    bool Force = false;
};

struct TLockUpdateRequest
{
    NYPath::TYPath Path;
    NTableClient::ELockType LockType;
};

struct TMethodRequest
{
    NYPath::TYPath Path;
    NYTree::INodePtr Value;
};

using TUpdateRequest = std::variant<
    TSetUpdateRequest,
    TRemoveUpdateRequest,
    TLockUpdateRequest,
    TMethodRequest
>;

struct TAttributeUpdateMatch
{
    const TScalarAttributeSchema* Schema;
    TUpdateRequest Request;
};

NYPath::TYPath GetUpdateRequestPath(const TUpdateRequest& updateRequest);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IQueryContext> MakeQueryContext(
    NMaster::IBootstrap* bootstrap,
    TObjectTypeValue objectType,
    ISession* session,
    const TScalarAttributeIndexDescriptor* indexedDescriptor = nullptr,
    bool allowAnnotations = true);

////////////////////////////////////////////////////////////////////////////////

struct TAttributeTimestampPrerequisite
{
    NYPath::TYPath Path;
    TTimestamp Timestamp;
};

////////////////////////////////////////////////////////////////////////////////

struct TObjectUpdateRequest
{
    TObject* Object;
    std::vector<TUpdateRequest> Updates;
    std::vector<TAttributeTimestampPrerequisite> Prerequisites;
    IAttributeValuesConsumer* Consumer = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

struct TUpdateIfExistingRequest
{
    std::vector<TUpdateRequest> Requests;
    std::vector<TAttributeTimestampPrerequisite> Prerequisites;
    IAttributeValuesConsumer* Consumer = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

struct TCreateObjectSubrequest
{
    TObjectTypeValue Type;
    NYTree::IMapNodePtr Attributes;
    std::optional<TUpdateIfExistingRequest> UpdateIfExisting;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionState,
    ((Active)       (0))
    ((Committing)   (1))
    ((Precommitted) (2))
    ((Committed)    (3))
    ((Failed)       (4))
    ((Aborted)      (5))
);

////////////////////////////////////////////////////////////////////////////////

struct ILoadContext
{
    virtual ~ILoadContext() = default;

    virtual const NTableClient::TRowBufferPtr& GetRowBuffer() = 0;
    virtual TString GetTablePath(const TDBTable* table) = 0;

    virtual void ScheduleLookup(
        const TDBTable* table,
        const TObjectKey& key,
        TRange<const TDBField*> fields,
        std::function<void(const std::optional<TRange<NTableClient::TUnversionedValue>>&)> handler) = 0;

    virtual void ScheduleLookup(
        const TDBTable* table,
        TRange<TObjectKey> keys,
        TRange<const TDBField*> fields,
        std::function<void(TSharedRange<NTableClient::TUnversionedRow>, const TDynBitMap&)> handler) = 0;

    virtual void ScheduleVersionedLookup(
        const TDBTable* table,
        const TObjectKey& key,
        TRange<const TDBField*> fields,
        std::function<void(const std::optional<TRange<NTableClient::TVersionedValue>>&)> handler) = 0;

    virtual void ScheduleVersionedLookup(
        const TDBTable* table,
        TRange<TObjectKey> keys,
        TRange<const TDBField*> fields,
        std::function<void(TSharedRange<NTableClient::TVersionedRow>, const TDynBitMap&)> handler) = 0;

    virtual void ScheduleSelect(
        std::string query,
        std::function<void(const NYT::NApi::IUnversionedRowsetPtr&)> handler,
        NTableClient::EVersionedIOMode versionedIOMode = NTableClient::EVersionedIOMode::Default) = 0;

    virtual void SchedulePullQueue(
        const TDBTable* table,
        std::optional<i64> offset,
        int partitionIndex,
        std::optional<i64> rowCountLimit,
        const std::optional<NYPath::TYPath>& consumer,
        std::function<void(const NQueueClient::IQueueRowsetPtr&)> handler) = 0;

    virtual void RunReads() = 0;
};

struct IStoreContext
{
    virtual ~IStoreContext() = default;

    virtual const NYT::NTableClient::TRowBufferPtr& GetRowBuffer() = 0;

    virtual void WriteRow(
        const TDBTable* table,
        const TObjectKey& tableKey,
        TRange<const TDBField*> fields,
        TRange<NYT::NTableClient::TUnversionedValue> values,
        bool sharedWrite = false) = 0;

    virtual void LockRow(
        const TDBTable* table,
        const TObjectKey& tableKey,
        TRange<const TDBField*> fields,
        NTableClient::ELockType lockType) = 0;

    virtual void DeleteRow(
        const TDBTable* table,
        const TObjectKey& tableKey) = 0;

    virtual void AdvanceConsumer(
        const TString& consumer,
        const TDBTable* queue,
        const std::optional<TString>& queueCluster,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset) = 0;

    virtual void FillTransaction() = 0;
};

struct IUpdateContext
{
    virtual ~IUpdateContext() = default;

    virtual void AddPreparer(std::function<void(IUpdateContext*)> preparer) = 0;
    virtual void AddFinalizer(std::function<void(IUpdateContext*)> finalizer) = 0;

    virtual void Commit() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TAttributeGroupingExpressions
{
    std::vector<TString> Expressions;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TAttributeGroupingExpressions& groupingExpression,
    TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct TAttributeAggregateExpressions
{
    std::vector<TString> Expressions;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TAttributeAggregateExpressions& groupingExpression,
    TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct TIndex
{
    TString Name;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TIndex& index,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TTimeInterval
{
    THistoryTime Begin;
    THistoryTime End;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TTimeInterval& timeInterval,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TAggregateQueryResult
{
    std::vector<TAttributeValueList> Objects;
};

////////////////////////////////////////////////////////////////////////////////

void SubscribeCommitted(
    const TYTTransactionDescriptor& descriptor,
    const NYT::NApi::ITransaction::TCommittedHandler& onCommitted);

void SubscribeAborted(
    const TYTTransactionDescriptor& descriptor,
    const NYT::NApi::ITransaction::TAbortedHandler& onAborted);

////////////////////////////////////////////////////////////////////////////////

NLogging::TLogger MakeTransactionLogger(TTransactionId transactionId);

////////////////////////////////////////////////////////////////////////////////

// Not thread-safe, use AcquireLock.
class TTransaction
    : public TRefCounted
{
public:
    TTransaction(
        NMaster::IBootstrap* bootstrap,
        TTransactionConfigsSnapshot configsSnapshot,
        TTransactionId id,
        TTimestamp startTimestamp,
        TYTTransactionOrClientDescriptor ytTransactionOrClient,
        std::string identityUserTag,
        TTransactionOptions options = TTransactionOptions());

    ~TTransaction();

    ETransactionState GetState() const;

    TTransactionId GetId() const;
    TTimestamp GetStartTimestamp() const;

    TInstant GetStartTime() const;
    TInstant GetCommitStartTime() const;
    TInstant GetCommitFinishTime() const;

    THistoryTime GetHistoryEventTime(const THistoryTableBase* history) const;

    void EnsureReadWrite() const;

    const TMutatingTransactionOptions& GetMutatingTransactionOptions() const;

    ISession* GetSession();
    NMaster::IBootstrap* GetBootstrap();
    TTransactionManagerConfigPtr GetConfig() const;

    std::unique_ptr<IUpdateContext> CreateUpdateContext();
    std::unique_ptr<ILoadContext> CreateLoadContext(
        TTestingStorageOptions testingStorageOptions);
    std::unique_ptr<IStoreContext> CreateStoreContext();

    std::vector<TObject*> CreateObjects(
        std::vector<TCreateObjectSubrequest> subrequests,
        const TTransactionCallContext& transactionCallContext = {});

    // Unlike the API-oriented |CreateObjects|, this does not schedule preloads, check access
    // controls or call attribute initializers. The calling code is responsible for performance,
    // permissions and validity.
    TObject* CreateObjectInternal(
        TObjectTypeValue type,
        TObjectKey key,
        TObjectKey parentKey = {},
        bool allowExisting = false);

    void RemoveObject(TObject* object);
    void RemoveObjects(std::vector<TObject*> objects);

    // Counterpart to |CreateObjectInternal|. Does not check access controls.
    void RemoveObjectInternal(TObject* object);

    void UpdateObject(
        TObject* object,
        std::vector<TUpdateRequest> requests,
        std::vector<TAttributeTimestampPrerequisite> prerequisites = {},
        IUpdateContext* context = nullptr,
        const TTransactionCallContext& transactionCallContext = {});
    void UpdateObjects(
        std::vector<TObjectUpdateRequest> objectUpdates,
        IUpdateContext* context = nullptr,
        const TTransactionCallContext& transactionCallContext = {});

    NYT::NApi::IUnversionedRowsetPtr SelectFields(
        TObjectTypeValue objectType,
        const std::vector<const TDBField*>& fields,
        std::source_location location = std::source_location::current());

    TObject* GetObject(
        TObjectTypeValue type,
        TObjectKey key,
        TObjectKey parentKey = {});

    template <class T>
    T* GetTypedObject(TObjectKey key, TObjectKey parentKey = {});

    //! After calling this method the transaction can only be committed or aborted.
    void RunPrecommitActions(TTransactionContext context);

    TFuture<TTransactionCommitResult> Commit(
        TTransactionContext context = TTransactionContext()) noexcept;

    //! Can be called concurrently with other methods.
    /*!
     *  Marks the transaction as aborted, thus forbidding any
     *  new changes to it from the object service,
     *  although allowing internal changes.
     *
     *  Aborts the underlying transaction.
     *
     *  Returns a future, which will be set when the underlying
     *  transaction abortion completes.
     */
    TFuture<void> Abort() noexcept;

    NConcurrency::TAsyncSemaphoreGuard AcquireLock();

    TPerformanceStatistics FlushPerformanceStatistics();
    const TPerformanceStatistics& GetTotalPerformanceStatistics();

    //! Transaction will schedule access control load on each instantiated object.
    //! By default preload is enabled for read-write transactions only.
    void EnableAccessControlPreload();
    bool AccessControlPreloadEnabled() const;

    void UpdateRequestTimeout(std::optional<TDuration> timeout);
    std::optional<TDuration> GetRequestTimeoutRemaining();

    // 0 - unlimited.
    void SetReadPhaseLimit(i64 limit);

    void AllowFullScan(bool allowFullScan);
    bool FullScanAllowed() const;

    void AllowRemovalWithNonEmptyReferences(bool allow);
    std::optional<bool> RemovalWithNonEmptyReferencesAllowed() const;

    using TAction = std::function<void()>;
    void AddAfterCommitAction(TAction action);
    void AddAfterAbortAction(TAction action);

    bool ScheduleCommitAction(TCommitActionType type, TObject* object);

    const std::optional<TString>& GetExecutionPoolTag() const;

    void PerformAttributeMigrations();

protected:
    THashSet<TObject*> ExtractCommitActions(TCommitActionType type);

    virtual void PrepareCommit();
    virtual void PostCommit();

private:
    class TImpl;
    using TImplPtr = TIntrusivePtr<TImpl>;
    const TImplPtr Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTransaction)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define TRANSACTION_INL_H_
#include "transaction-inl.h"
#undef TRANSACTION_INL_H
