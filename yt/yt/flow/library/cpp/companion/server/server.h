#pragma once

#include "public.h"

#include "pipeline.h"

#include <yt/yt/flow/library/cpp/companion/config.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/library/profiling/solomon/public.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! Owns the companion gRPC server and its worker thread pool.
class TCompanionServer
    : public TRefCounted
{
public:
    //! |registry| defaults to the process-wide sensor registry.
    TCompanionServer(
        NCompanion::TCompanionExecutionConfigPtr config,
        TPipeline pipeline,
        NProfiling::TSolomonRegistryPtr registry = nullptr);

    void Start();
    void Stop();

    //! Monitoring server; inert when disabled.
    const TCompanionMonitoringPtr& GetMonitoring() const;

private:
    const NCompanion::TCompanionExecutionConfigPtr Config_;
    const TCompanionMonitoringPtr Monitoring_;
    NConcurrency::IThreadPoolPtr ThreadPool_;
    NRpc::IServerPtr RpcServer_;
};

DEFINE_REFCOUNTED_TYPE(TCompanionServer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
