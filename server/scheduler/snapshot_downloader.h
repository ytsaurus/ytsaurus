#pragma once

#include "private.h"

#include <yt/server/cell_scheduler/public.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotDownloader
    : public TRefCounted
{
public:
    explicit TSnapshotDownloader(
        TSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap,
        TOperationPtr operation);

    void Run();

private:
    TSchedulerConfigPtr Config;
    NCellScheduler::TBootstrap* Bootstrap;
    TOperationPtr Operation;

    NLogging::TLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
