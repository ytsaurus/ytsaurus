#pragma once

#include "private.h"

#include <yt/core/actions/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/ytree/public.h>

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
        THost* host);

    void Start();

    void OnProfiling();

private:
    const THealthCheckerConfigPtr Config_;
    const TString DatabaseUser_;
    const DB::Context* const DatabaseContext_;
    THost* const Host_;
    NConcurrency::TActionQueuePtr ActionQueue_;
    const NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
    std::vector<NProfiling::TTagId> QueryIndexToTag_;

    // Profiling should be exported at least once per 10 seconds
    // (according to current setup for all YT Solomon services)
    // but health check queries may last longer. That's why we export
    // last check values until the new ones arrive.
    // Values are set from ActionQueue_ and are read from control invoker,
    // hence access them under spin lock.
    std::vector<bool> LastResult_;
    TSpinLock Lock_;

    void ExecuteQuery(const TString& query);
    void ExecuteQueries();
};

DEFINE_REFCOUNTED_TYPE(THealthChecker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
