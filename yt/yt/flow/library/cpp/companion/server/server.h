#pragma once

#include "public.h"

#include "pipeline.h"

#include <yt/yt/flow/library/cpp/companion/config.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! Owns the companion gRPC server and its worker thread pool.
class TCompanionServer
    : public TRefCounted
{
public:
    TCompanionServer(
        NCompanion::TCompanionExecutionConfigPtr config,
        TPipeline pipeline);

    void Start();
    void Stop();

private:
    const NCompanion::TCompanionExecutionConfigPtr Config_;
    NConcurrency::IThreadPoolPtr ThreadPool_;
    NRpc::IServerPtr RpcServer_;
};

DEFINE_REFCOUNTED_TYPE(TCompanionServer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
