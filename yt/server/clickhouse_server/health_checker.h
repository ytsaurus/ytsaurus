#pragma once

#include <yt/core/actions/public.h>
#include <yt/core/concurrency/public.h>
#include <yt/core/ytree/public.h>

#include <yt/server/clickhouse_server/private.h>

#include <contrib/libs/clickhouse/dbms/src/Core/Block.h>
#include <contrib/libs/clickhouse/dbms/src/DataStreams/BlockIO.h>
#include <contrib/libs/clickhouse/dbms/src/Interpreters/Context.h>

namespace NYT::NClickHouseServer {

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
    THealthCheckerConfigPtr Config_;
    TString DataBaseUser_;
    const DB::Context* DatabaseContext_;
    TBootstrap* Bootstrap_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
    std::vector<NProfiling::TTagId> QueryIndexToTag_;

    void ExecuteQuery(const TString& query);
    void ExecuteAndProfileQueries();
};

DEFINE_REFCOUNTED_TYPE(THealthChecker);

} // namespace NYT::NClickHouseServer
