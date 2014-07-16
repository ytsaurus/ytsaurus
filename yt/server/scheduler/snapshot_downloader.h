#pragma once

#include "private.h"

#include <core/logging/log.h>

#include <server/cell_scheduler/public.h>

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

    NLog::TLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
