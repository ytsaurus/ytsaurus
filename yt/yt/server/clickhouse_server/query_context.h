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

    TStorageContext(int index, const DB::Context& context, TQueryContext* queryContext);

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
    NProfiling::TTagId UserTagId;
    const NTracing::TTraceContextPtr TraceContext;
    const TQueryId QueryId;
    const EQueryKind QueryKind;
    THost* const Host;
    TString Query;
    TString CurrentUser;
    TString CurrentAddress;
    std::optional<TString> InitialUser;
    std::optional<TString> InitialAddress;
    std::optional<TQueryId> InitialQueryId;
    //! Text of the initial query. Used for better debugging.
    std::optional<TString> InitialQuery;
    EInterface Interface;
    TString ClientHostName;
    std::optional<TString> HttpUserAgent;
    std::optional<TString> DataLensRequestId;

    NTableClient::TRowBufferPtr RowBuffer;

    TQuerySettingsPtr Settings;

    TQueryContext(
        THost* host,
        const DB::Context& context,
        TQueryId queryId,
        NTracing::TTraceContextPtr traceContext,
        std::optional<TString> dataLensRequestId);

    ~TQueryContext();

    const NApi::NNative::IClientPtr& Client() const;

    void MoveToPhase(EQueryPhase phase);

    EQueryPhase GetQueryPhase() const;

    TStorageContext* FindStorageContext(const DB::IStorage* storage);
    TStorageContext* GetOrRegisterStorageContext(const DB::IStorage* storage, const DB::Context& context);

private:
    TInstant StartTime_;

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
    DB::Context& context,
    TQueryId queryId,
    NTracing::TTraceContextPtr traceContext,
    std::optional<TString> dataLensRequestId = std::nullopt);

TQueryContext* GetQueryContext(const DB::Context& context);

NLogging::TLogger GetLogger(const DB::Context& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
