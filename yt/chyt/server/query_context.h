#pragma once

#include "private.h"

#include "conversion.h"

#include <yt/yt/ytlib/api/native/client_cache.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/statistics.h>

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

DEFINE_REFCOUNTED_TYPE(TStorageContext);

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

    // Fields for a statistics reporter.
    std::vector<TString> SelectQueries;
    std::vector<TString> SecondaryQueryIds;
    //! Statistics for 'simple' query.
    TStatistics InstanceStatistics;
    //! Aggregated statistics from all subquery. InstanceStatistics is merged in the end of the query.
    TStatistics AggregatedStatistics;
    //! Index of this select in the parent query (=Storage index).
    int SelectQueryIndex;

    NTableClient::TRowBufferPtr RowBuffer;

    TQuerySettingsPtr Settings;

    TQueryContext(
        THost* host,
        DB::ContextPtr context,
        TQueryId queryId,
        NTracing::TTraceContextPtr traceContext,
        std::optional<TString> dataLensRequestId,
        const TSubqueryHeaderPtr& subqueryHeader);

    ~TQueryContext();

    const NApi::NNative::IClientPtr& Client() const;

    void MoveToPhase(EQueryPhase phase);

    EQueryPhase GetQueryPhase() const;

    // TODO(dakovalkov): Move here logic from destructor?
    void Finish();

    TInstant GetStartTime() const;
    TInstant GetFinishTime() const;

    TStorageContext* FindStorageContext(const DB::IStorage* storage);
    TStorageContext* GetOrRegisterStorageContext(const DB::IStorage* storage, DB::ContextPtr context);

private:
    TInstant StartTime_;
    TInstant FinishTime_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, PhaseLock_);
    std::atomic<EQueryPhase> QueryPhase_ {EQueryPhase::Start};
    TInstant LastPhaseTime_;
    TString PhaseDebugString_ = ToString(EQueryPhase::Start);

    //! Spinlock controlling lazy client creation.
    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, ClientLock_);

    //! Native client for the user that initiated the query. Created on first use.
    mutable NApi::NNative::IClientPtr Client_;

    //! Spinlock controlling select query context map.
    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, StorageToStorageContextLock_);
    THashMap<const DB::IStorage*, TStorageContextPtr> StorageToStorageContext_;
};

DEFINE_REFCOUNTED_TYPE(TQueryContext);

void Serialize(const TQueryContext& queryContext, NYson::IYsonConsumer* consumer, const DB::QueryStatusInfo* queryStatusInfo);

////////////////////////////////////////////////////////////////////////////////

void SetupHostContext(
    THost* host,
    DB::ContextPtr context,
    TQueryId queryId,
    NTracing::TTraceContextPtr traceContext,
    std::optional<TString> dataLensRequestId = std::nullopt,
    const TSubqueryHeaderPtr& subqueryHeader = nullptr);

TQueryContext* GetQueryContext(DB::ContextPtr context);

NLogging::TLogger GetLogger(DB::ContextPtr context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
