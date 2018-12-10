#pragma once

#include "private.h"
#include "operation_controller.h"

#include <yt/server/scheduler/public.h>

#include <yt/server/misc/fork_executor.h>

#include <yt/client/api/public.h>

#include <yt/core/pipes/pipe.h>

#include <yt/core/profiling/profiler.h>

#include <util/system/file.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct TSnapshotJob
    : public TIntrinsicRefCounted
{
    TOperationId OperationId;
    // We intentionally deal with controller weak pointers in the parent process in order
    // to not prolong controller lifetime.
    IOperationControllerWeakPtr WeakController;
    NConcurrency::IAsyncInputStreamPtr Reader;
    std::unique_ptr<TFile> OutputFile;
    TSnapshotCookie Cookie;
    bool Suspended = false;
};

DEFINE_REFCOUNTED_TYPE(TSnapshotJob)

////////////////////////////////////////////////////////////////////////////////

class TSnapshotBuilder
    : public TForkExecutor
{
public:
    TSnapshotBuilder(
        TControllerAgentConfigPtr config,
        NApi::IClientPtr client,
        IInvokerPtr ioInvoker,
        const TIncarnationId& incarnationId);

    TFuture<void> Run(const TOperationIdToWeakControllerMap& controllers);

private:
    const TControllerAgentConfigPtr Config_;
    const NApi::IClientPtr Client_;
    const IInvokerPtr IOInvoker_;
    const IInvokerPtr ControlInvoker_;
    const TIncarnationId IncarnationId_;

    std::vector<TSnapshotJobPtr> Jobs_;

    NProfiling::TProfiler Profiler;

    //! This method is called after controller is suspended.
    //! It is used to set flag Suspended in corresponding TSnapshotJob.
    void OnControllerSuspended(const TSnapshotJobPtr& job);

    virtual TDuration GetTimeout() const override;
    virtual void RunParent() override;
    virtual void RunChild() override;

    TFuture<std::vector<TError>> UploadSnapshots();
    void UploadSnapshot(const TSnapshotJobPtr& job);

    bool ControllersSuspended_ = false;
};

DEFINE_REFCOUNTED_TYPE(TSnapshotBuilder)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
