#pragma once

#include "public.h"

#include <yt/server/cell_scheduler/public.h>

#include <yt/server/misc/fork_executor.h>

#include <yt/ytlib/api/public.h>

#include <yt/core/pipes/pipe.h>

#include <util/system/file.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotBuilder
    : public TForkExecutor
{
public:
    TSnapshotBuilder(
        TSchedulerConfigPtr config,
        TSchedulerPtr scheduler,
        NApi::IClientPtr client);

    TFuture<void> Run();

private:
    const TSchedulerConfigPtr Config_;
    const TSchedulerPtr Scheduler_;
    const NApi::IClientPtr Client_;

    struct TJob
    {
        TOperationPtr Operation;
        NPipes::TAsyncReaderPtr Reader;
        std::unique_ptr<TFile> OutputFile;
    };

    std::vector<TJob> Jobs_;


    virtual TDuration GetTimeout() const override;
    virtual void RunParent() override;
    virtual void RunChild() override;

    TFuture<void> UploadSnapshots();
    void UploadSnapshot(const TJob& job);

};

DEFINE_REFCOUNTED_TYPE(TSnapshotBuilder)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
