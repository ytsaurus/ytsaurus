#pragma once

#include "private.h"

#include "conversion.h"
#include "cluster_nodes.h"
#include "object_lock.h"

#include <yt/yt/ytlib/api/native/client_cache.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/statistics.h>

#include <library/cpp/yt/threading/spin_lock.h>
#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <Interpreters/Context.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TQueryContext;

//! Context for select query.
struct TStorageContext
    : public TRefCounted
{
public:
    int Index = -1;
    TQueryContext* QueryContext;
    TQuerySettingsPtr Settings;
    NLogging::TLogger Logger;

    TStorageContext(int index, DB::ContextPtr context, TQueryContext* queryContext);

    ~TStorageContext();
};

DEFINE_REFCOUNTED_TYPE(TStorageContext)

////////////////////////////////////////////////////////////////////////////////

//! Context for whole query. Shared by all select queries from YT tables in query
//! (including subqueries).
struct TQueryContext
    : public TRefCounted
{
public:
    NLogging::TLogger Logger;
    const TString User;

    const NTracing::TTraceContextPtr TraceContext;
    const TQueryId QueryId;
    const EQueryKind QueryKind;
    THost* const Host;
    TString Query;
    TString CurrentUser;
    TString CurrentAddress;
    TString InitialUser;
    TString InitialAddress;
    TQueryId InitialQueryId;
    std::optional<TQueryId> ParentQueryId;
    //! Text of the initial query. Used for better debugging.
    std::optional<TString> InitialQuery;
    EInterface Interface;
    std::optional<TString> HttpUserAgent;
    std::optional<TString> DataLensRequestId;
    std::optional<TString> YqlOperationId;

    // Transactionality
    //! ReadTransactionId is the id of the query transaction in which snapshot locks are taken.
    NTransactionClient::TTransactionId ReadTransactionId;
    //! Acquired snapshot locks under a read transaction.
    //! For fully consistent read under the transaction a node id from the lock should be used instead of a node path.
    //! Can contain an empty TObjectLock object if the lock is acquired asynchronously.
    THashMap<NYPath::TYPath, TObjectLock> SnapshotLocks;
    //! DynamicTableReadTimestamp is used for dynamic tables if snapshot locks are taken.
    NTransactionClient::TTimestamp DynamicTableReadTimestamp = NTransactionClient::AsyncLastCommittedTimestamp;
    //! WriteTransactionId is the id of the query transaction in which all write operations should be performed.
    NTransactionClient::TTransactionId WriteTransactionId;
    //! CreatedTablePath is the path of the table created in write transaction.
    std::optional<NYPath::TYPath> CreatedTablePath;

    // Fields for a statistics reporter.
    std::vector<TString> SelectQueries;
    std::vector<TString> SecondaryQueryIds;
    //! Statistics for 'simple' query.
    TStatistics InstanceStatistics;
    //! Aggregated statistics from all subquery. InstanceStatistics is merged in the end of the query.
    TStatistics AggregatedStatistics;
    //! Index of this select in the parent query.
    int SelectQueryIndex = 0;

    //! Level of the query in an execution tree.
    int QueryDepth = 0;

    NTableClient::TRowBufferPtr RowBuffer;

    TQuerySettingsPtr Settings;

    TQueryContext(
        THost* host,
        DB::ContextPtr context,
        TQueryId queryId,
        NTracing::TTraceContextPtr traceContext,
        std::optional<TString> dataLensRequestId,
        std::optional<TString> yqlOperationId,
        const TSecondaryQueryHeaderPtr& secondaryQueryHeader);

    ~TQueryContext();

    // TODO(dakovalkov): Try to eliminate this.
    //! Create fake query context.
    //! Fake context is used only to fetch tables in dictionary source
    //! because real query context is not available through ClickHouse interface.
    //! Fake context initializes only fields which are used in fetching tables.
    //! Fake context has QueryKind = EQueryKind::NoQuery.
    static TQueryContextPtr CreateFake(THost* host, NApi::NNative::IClientPtr client);

    const NApi::NNative::IClientPtr& Client() const;

    void MoveToPhase(EQueryPhase phase);

    EQueryPhase GetQueryPhase() const;

    // TODO(dakovalkov): Move here logic from destructor?
    void Finish();

    TInstant GetStartTime() const;
    TInstant GetFinishTime() const;

    TStorageContext* FindStorageContext(const DB::IStorage* storage);
    TStorageContext* GetOrRegisterStorageContext(const DB::IStorage* storage, DB::ContextPtr context);

    const TClusterNodes& GetClusterNodesSnapshot();

    std::vector<TErrorOr<NYTree::IAttributeDictionaryPtr>> GetObjectAttributesSnapshot(
        const std::vector<NYPath::TYPath>& paths);
    void DeleteObjectAttributesFromSnapshot(const std::vector<NYPath::TYPath>& paths);

    // Transactionality
    void InitializeQueryWriteTransaction();
    void CommitWriteTransaction();

    //! If the transaction is created asynchronously, waits for its future and
    //! fills ReadTransactionId and DynamicTableReadTimestamp fields.
    void SaveQueryReadTransaction();
    //! Try to get node id from PathToNodeId, return path on failure.
    //! Node id format is "#<node_id>".
    NYPath::TYPath GetNodeIdOrPath(const NYPath::TYPath& path) const;

    //! Synchronously acquire snapshot locks on given paths under the read transaction.
    void AcquireSnapshotLocks(const std::vector<NYPath::TYPath>& paths);

private:
    TInstant StartTime_;
    TInstant FinishTime_;

    //! Snapshot of the cluster nodes to avoid races.
    //! Access through GetClusterNodesSnapshot.
    std::optional<TClusterNodes> ClusterNodesSnapshot_;
    //! Snapshot of the object attributes. Saving it here has several purposes:
    //! 1) Every part of the query always sees the same object attributes (avoiding races).
    //! 2) It acts like a per-query cache to avoid many master request when per-clique cache is disabled.
    THashMap<NYPath::TYPath, TErrorOr<NYTree::IAttributeDictionaryPtr>> ObjectAttributesSnapshot_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, PhaseLock_);
    std::atomic<EQueryPhase> QueryPhase_ {EQueryPhase::Start};
    TInstant LastPhaseTime_;
    TString PhaseDebugString_ = ToString(EQueryPhase::Start);

    //! Spinlock controlling lazy client creation.
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ClientLock_);
    //! Native client for the user that initiated the query. Created on first use.
    mutable NApi::NNative::IClientPtr Client_;

    //! Spinlock controlling select query context map.
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, StorageToStorageContextLock_);
    THashMap<const DB::IStorage*, TStorageContextPtr> StorageToStorageContext_;

    // Transactionality

    //! InitialQueryReadTransaction and InitialQueryWriteTransaction are initialized
    //! only on the initiator to ping the transactions during the query execution.
    NApi::NNative::ITransactionPtr InitialQueryReadTransaction_;
    NApi::NNative::ITransactionPtr InitialQueryWriteTransaction_;

    struct TTransactionWithTimestamp
    {
        NApi::NNative::ITransactionPtr Transaction;
        NTransactionClient::TTimestamp Timestamp;
    };

    TFuture<TTransactionWithTimestamp> ReadTransactionFuture_;

    void InitializeQueryReadTransactionFuture();

    //! Constructs fake query context.
    //! It's private to avoid creating it accidently.
    TQueryContext(THost* host, NApi::NNative::IClientPtr client);

    //! Fetch table attributes from master asynchronously.
    TFuture<std::vector<TErrorOr<NYTree::IAttributeDictionaryPtr>>> FetchTableAttributesAsync(
        const std::vector<NYPath::TYPath>& paths,
        NTransactionClient::TTransactionId transactionId);

    //! Fetch table attributes from master synchronously.
    std::vector<TErrorOr<NYTree::IAttributeDictionaryPtr>> FetchTableAttributes(
        const std::vector<NYPath::TYPath>& paths,
        NTransactionClient::TTransactionId transactionId);

    TFuture<std::vector<TErrorOr<TObjectLock>>> DoAcquireSnapshotLocks(
        const std::vector<NYPath::TYPath>& paths);

    DECLARE_NEW_FRIEND()
};

DEFINE_REFCOUNTED_TYPE(TQueryContext)

void Serialize(const TQueryContext& queryContext, NYson::IYsonConsumer* consumer, const DB::QueryStatusInfo* queryStatusInfo);

////////////////////////////////////////////////////////////////////////////////

void SetupHostContext(
    THost* host,
    DB::ContextMutablePtr context,
    TQueryId queryId,
    NTracing::TTraceContextPtr traceContext,
    std::optional<TString> dataLensRequestId = std::nullopt,
    std::optional<TString> yqlOperationId = std::nullopt,
    const TSecondaryQueryHeaderPtr& secondaryQueryHeader = nullptr);

TQueryContext* GetQueryContext(DB::ContextPtr context);

NLogging::TLogger GetLogger(DB::ContextPtr context);

void InvalidateCache(
    TQueryContext* queryContext,
    std::vector<NYPath::TYPath> paths,
    std::optional<EInvalidateCacheMode> invalidateMode = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
