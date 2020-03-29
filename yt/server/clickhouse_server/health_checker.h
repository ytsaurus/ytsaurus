#pragma once

#include <yt/server/clickhouse_server/private.h>

#include <yt/core/actions/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/ytree/public.h>

#include <contrib/libs/clickhouse/dbms/src/Core/Block.h>

#include <contrib/libs/clickhouse/dbms/src/DataStreams/BlockIO.h>

#include <contrib/libs/clickhouse/dbms/src/Interpreters/Context.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class THealthChecker
    : public TRefCounted
{
public:
    THealthChecker(
        THealthCheckerConfigPtr config,
        TString dataBaseUser,
        const DB::Context* databaseContext,
        TBootstrap* bootstrap);

    void Start();

private:
    const THealthCheckerConfigPtr Config_;
    const TString DatabaseUser_;
    const DB::Context* const DatabaseContext_;
    TBootstrap* const Bootstrap_;
    NConcurrency::TActionQueuePtr ActionQueue_;
    const NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
    std::vector<NProfiling::TTagId> QueryIndexToTag_;

    void ExecuteQuery(const TString& query);
    void ExecuteAndProfileQueries();
};

DEFINE_REFCOUNTED_TYPE(THealthChecker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
