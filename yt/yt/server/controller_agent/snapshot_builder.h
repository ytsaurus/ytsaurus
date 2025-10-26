#pragma once

#include "private.h"
#include "operation_controller.h"

#include <yt/yt/server/lib/misc/fork_executor.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/library/pipe_io/pipe.h>

#include <util/system/file.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct TSnapshotJob
    : public TRefCounted
{
    TOperationId OperationId;
    // We intentionally deal with controller weak pointers in the parent process in order
    // to not prolong controller lifetime.
    IOperationControllerWeakPtr WeakController;
    NConcurrency::IAsyncZeroCopyInputStreamPtr Reader;
    std::unique_ptr<TFile> OutputFile;
    TSnapshotCookie Cookie;
    bool Suspended = false;
};

DEFINE_REFCOUNTED_TYPE(TSnapshotJob)

////////////////////////////////////////////////////////////////////////////////

class TSnapshotBuilder
    : public NServer::TForkExecutor
{
public:
    TSnapshotBuilder(
        TControllerAgentConfigPtr config,
        NApi::IClientPtr client,
        IInvokerPtr ioInvoker,
        TIncarnationId incarnationId,
        NServer::TForkCountersPtr counters);

    TFuture<void> Run(const TOperationIdToWeakControllerMap& controllers);

private:
    const TControllerAgentConfigPtr Config_;
    const NApi::IClientPtr Client_;
    const IInvokerPtr IOInvoker_;
    const IInvokerPtr ControlInvoker_;
    const TIncarnationId IncarnationId_;

    std::vector<TSnapshotJobPtr> Jobs_;

    //! This method is called after controller is suspended.
    //! It is used to set flag Suspended in corresponding TSnapshotJob.
    void OnControllerSuspended(const TSnapshotJobPtr& job);

    TDuration GetTimeout() const override;
    TDuration GetForkTimeout() const override;
    void RunParent() override;
    void RunChild() override;

    TFuture<std::vector<TError>> UploadSnapshots();
    void UploadSnapshot(const TSnapshotJobPtr& job);

    bool ControllersSuspended_ = false;
};

DEFINE_REFCOUNTED_TYPE(TSnapshotBuilder)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
