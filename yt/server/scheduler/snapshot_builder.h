#pragma once

#include "public.h"

#include <ytlib/api/public.h>

#include <server/misc/fork_snapshot_builder.h>

#include <server/cell_scheduler/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotBuilder
    : public TForkSnapshotBuilderBase
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
        Stroka TempFileName;
        Stroka FileName;
    };

    std::vector<TJob> Jobs_;


    virtual TDuration GetTimeout() const override;
    virtual void RunChild() override;
    
    void Build(const TJob& job);
    void OnBuilt();

    void UploadSnapshot(const TJob& job);

};

DEFINE_REFCOUNTED_TYPE(TSnapshotBuilder)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
