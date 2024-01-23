#pragma once

#include <yt/yt/server/lib/alert_manager/alert_manager.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>
#include <yt/yt/ytlib/api/native/type_handler.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/queue_client/config.h>

#include <yt/yt/library/auth/auth.h>

#include <yt/yt/core/misc/error_code_counter.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

struct TQueueExportProfilingCounters final
{
    NProfiling::TCounter ExportedRows;
    NProfiling::TCounter ExportedChunks;
    NProfiling::TCounter ExportedTables;

    explicit TQueueExportProfilingCounters(const NProfiling::TProfiler& profiler);
};

using TQueueExportProfilingCountersPtr = TIntrusivePtr<TQueueExportProfilingCounters>;

////////////////////////////////////////////////////////////////////////////////

class TQueueExporter
    : public TRefCounted
{
public:
    TQueueExporter() = default;

    TQueueExporter(
        TString exportName,
        NHiveClient::TClientDirectoryPtr clientDirectory,
        IInvokerPtr invoker,
        NAlertManager::IAlertCollectorPtr alertCollector,
        const NProfiling::TProfiler& queueProfiler,
        const NLogging::TLogger& logger);

    TFuture<void> RunExportIteration(
        const NQueueClient::TCrossClusterReference& queue,
        const NQueueClient::TQueueStaticExportConfig& config);

private:
    const TString ExportName_;
    const NHiveClient::TClientDirectoryPtr ClientDirectory_;
    const IInvokerPtr Invoker_;
    const NAlertManager::IAlertCollectorPtr AlertCollector_;
    const TQueueExportProfilingCountersPtr ProfilingCounters_;

    const NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TQueueExporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
