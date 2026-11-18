#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/queue_client/queue_export.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/actions/current_invoker.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NApi::NInternalProxy {

////////////////////////////////////////////////////////////////////////////////

//! The greatest export unix ts of #schedule not exceeding #now, i.e. the most recent exported table that
//! can be created at #now.
ui64 GetExportUnixTsUpperBound(TInstant now, const NQueueClient::TQueueExportSchedule& schedule);

//! Calculate how long the export period before timestamp #exportUnixTs was, according to #schedule.
//!
//! \note Function throws if #exportUnixTs is not a valid export timestamp.
ui64 GetLastExportPeriod(ui64 exportUnixTs, const NQueueClient::TQueueExportSchedule& schedule);

////////////////////////////////////////////////////////////////////////////////

//! Exports deltas of the queue data into static tables in-process.
class TQueueExportRunner
{
public:
    explicit TQueueExportRunner(TQueueExportRunnerConfigPtr config);

    //! Exports a delta of the queue data into static tables, using #client for all the requests.
    //! Each tablet is exported right after its chunk in #lastExportedChunkIds, or from the beginning if there is none.
    //! #startOffsets are only used for verification.
    TFuture<NQueueClient::TExportQueueIntoStaticTableResult> ExportQueueIntoStaticTable(
        NNative::IClientPtr client,
        NYPath::TYPath queuePath,
        NYPath::TYPath exportDirectory,
        THashMap<i64, NChunkClient::TChunkId> lastExportedChunkIds,
        THashMap<i64, i64> startOffsets,
        NTransactionClient::TTimestamp startTimestamp,
        NQueueClient::TQueueExportSchedule schedule,
        NQueueClient::TExportQueueIntoStaticTableOptions options,
        NLogging::TLogger logger,
        IInvokerPtr invoker = GetCurrentInvoker()) const;

private:
    const TQueueExportRunnerConfigPtr Config_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NInternalProxy
