#pragma once

#include "private.h"
#include "operation_controller.h"

#include <yt/server/scheduler/public.h>

#include <yt/server/misc/fork_executor.h>

#include <yt/ytlib/api/public.h>

#include <yt/core/pipes/pipe.h>

#include <yt/core/profiling/profiler.h>

#include <util/system/file.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct TSnapshotJob
    : public TIntrinsicRefCounted
{
    TOperationId OperationId;
    IOperationControllerSnapshotBuilderHostPtr Controller;
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
        TOperationIdToOperationMap operations,
        NApi::IClientPtr client,
        IInvokerPtr ioInvoker);

    TFuture<void> Run();

private:
    const TControllerAgentConfigPtr Config_;
    const TOperationIdToOperationMap Operations_;
    const NApi::IClientPtr Client_;
    const IInvokerPtr IOInvoker_;
    const IInvokerPtr ControlInvoker_;

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

} // namespace NControllerAgent
} // namespace NYT
