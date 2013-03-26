#pragma once

#include "private.h"

#include <ytlib/logging/tagged_logger.h>

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

    TFuture<void> Run();

private:
    TSchedulerConfigPtr Config;
    NCellScheduler::TBootstrap* Bootstrap;
    TOperationPtr Operation;

    NLog::TTaggedLogger Logger;

    TVoid Download();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
