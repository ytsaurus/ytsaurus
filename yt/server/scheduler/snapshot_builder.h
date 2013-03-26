#pragma once

#include "public.h"

#include <server/misc/snapshot_builder_detail.h>

#include <server/cell_scheduler/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotBuilder
    : public TSnapshotBuilderBase
{
public:
    TSnapshotBuilder(
        TSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap);

    TAsyncError Run();

private:
    TSchedulerConfigPtr Config;
    NCellScheduler::TBootstrap* Bootstrap;

    struct TJob
    {
        TOperationPtr Operation;
        Stroka TempFileName;
        Stroka FileName;
    };

    std::vector<TJob> Jobs;

    virtual TDuration GetTimeout() const override;
    virtual void Build() override;
    
    void Build(const TJob& job);

    TError OnBuilt(TError error);

    void UploadSnapshot(const TJob& job);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
