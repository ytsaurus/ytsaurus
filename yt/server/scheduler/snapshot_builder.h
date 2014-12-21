#pragma once

#include "public.h"

#include <ytlib/api/public.h>

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
        TSchedulerPtr scheduler,
        NApi::IClientPtr masterClient);

    TAsyncError Run();

private:
    TSchedulerConfigPtr Config;
    TSchedulerPtr Scheduler;
    NApi::IClientPtr MasterClient;

    struct TJob
    {
        TOperationPtr Operation;
        Stroka TempFileName;
        Stroka FileName;
    };

    std::vector<TJob> Jobs;

    virtual TDuration GetTimeout() const override;
    virtual void RunChild() override;
    
    void Build(const TJob& job);

    TError OnBuilt(TError error);

    void UploadSnapshot(const TJob& job);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
