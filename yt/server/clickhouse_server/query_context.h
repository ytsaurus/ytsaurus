#pragma once

#include "private.h"

#include "schema.h"

#include <yt/ytlib/api/native/client_cache.h>

#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/client/table_client/schema.h>

#include <yt/client/table_client/row_buffer.h>

#include <yt/core/concurrency/public.h>
#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/ytree/public.h>

#include <yt/core/logging/log.h>

#include <Interpreters/Context.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

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

private:
    NTracing::TTraceContextGuard TraceContextGuard_;

    TInstant StartTime_;

    mutable TSpinLock PhaseLock_;
    std::atomic<EQueryPhase> QueryPhase_ {EQueryPhase::Start};
    TInstant LastPhaseTime_;
    TString PhaseDebugString_ = ToString(EQueryPhase::Start);

    //! Spinlock controlling lazy client creation.
    mutable NConcurrency::TReaderWriterSpinLock ClientLock_;

    //! Native client for the user that initiated the query. Created on first use.
    mutable NApi::NNative::IClientPtr Client_;
};

void Serialize(const TQueryContext& queryContext, NYson::IYsonConsumer* consumer, const DB::QueryStatusInfo* queryStatusInfo);

////////////////////////////////////////////////////////////////////////////////

void SetupHostContext(
    THost* host,
    DB::Context& context,
    TQueryId queryId,
    NTracing::TTraceContextPtr traceContext,
    std::optional<TString> dataLensRequestId = std::nullopt);

TQueryContext* GetQueryContext(const DB::Context& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
