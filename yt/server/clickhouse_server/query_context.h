#pragma once

#include "private.h"
#include "bootstrap.h"
#include "host.h"

#include "document.h"
#include "objects.h"
#include "table_schema.h"

#include <yt/client/table_client/schema.h>

#include <yt/ytlib/api/native/client_cache.h>

#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/client/table_client/row_buffer.h>

#include <yt/core/concurrency/public.h>
#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/ytree/public.h>

#include <yt/core/logging/log.h>

#include <Interpreters/Context.h>

namespace NYT::NClickHouseServer {

using namespace NLogging;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

struct TQueryContext
    : public DB::IHostContext
{
public:
    TLogger Logger;
    const TString User;
    const TQueryId QueryId;
    const EQueryKind QueryKind;
    TBootstrap* const Bootstrap;
    TString Query;
    TString CurrentUser;
    TString CurrentAddress;
    std::optional<TString> InitialUser;
    std::optional<TString> InitialAddress;
    std::optional<TQueryId> InitialQueryId;
    EInterface Interface;
    TString ClientHostName;
    std::optional<TString> HttpUserAgent;

    NTableClient::TRowBufferPtr RowBuffer;

    explicit TQueryContext(TBootstrap* bootstrap, TQueryId queryId, const DB::Context& context);

    ~TQueryContext();

    const NApi::NNative::IClientPtr& Client() const;

    DB::QueryStatus* TryGetQueryStatus() const;

private:
    TClickHouseHostPtr Host_;

    //! Spinlock controlling lazy client creation.
    mutable TReaderWriterSpinLock ClientLock_;

    //! Native client for the user that initiated the query. Created on first use.
    mutable NApi::NNative::IClientPtr Client_;
};

void Serialize(const TQueryContext& queryContext, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

void SetupHostContext(TBootstrap* bootstrap, DB::Context& context, TQueryId queryId);

TQueryContext* GetQueryContext(const DB::Context& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
