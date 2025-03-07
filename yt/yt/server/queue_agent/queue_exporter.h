#pragma once

#include "private.h"
#include "config.h"

#include <yt/yt/server/lib/alert_manager/alert_manager.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>
#include <yt/yt/ytlib/api/native/type_handler.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/queue_client/config.h>

#include <yt/yt/library/auth/auth.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

class TQueueTabletExportProgress
    : public NYTree::TYsonStruct
{
public:
    NChunkClient::TChunkId LastChunk;
    NHiveClient::TTimestamp MaxTimestamp;
    i64 RowCount;
    i64 ChunkCount;

    REGISTER_YSON_STRUCT(TQueueTabletExportProgress);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueTabletExportProgress)

////////////////////////////////////////////////////////////////////////////////

class TQueueExportProgress
    : public NYTree::TYsonStruct
{
public:
    //! Instant corresponding to the last export task, which had no errors, meaning
    //! that all exported tables supposed to be created were created without issues or
    //! at the point of the task execution there was nothing to export.
    TInstant LastSuccessfulExportTaskInstant;
    // COMPAT(apachee): Duplicate of LastSuccessfulExportTaskInstant, used for compatability.
    TInstant LastSuccessfulExportIterationInstant;
    //! Instant corresponding to the last export task executed. This task might have been successful (as in #LastSuccessfulExportTaskInstant),
    //! able to create some (or none) of the supposed-to-be-created exported tables due to some error.
    //! Always greater or equal to #LastSuccessfulExportTaskInstant.
    TInstant LastExportTaskInstant;
    //! Export unix ts of the last exported table.
    //! It is a multiple of the export period passed to the export task, which created the last exported table.
    ui64 LastExportUnixTs;
    THashMap<i64, TQueueTabletExportProgressPtr> Tablets;
    //! Queue id corresponding to this export progress.
    NObjectClient::TObjectId QueueObjectId;

    void Update(i64 tabletIndex, NChunkClient::TChunkId chunkId, NHiveClient::TTimestamp maxTimestamp, i64 rowCount);

    REGISTER_YSON_STRUCT(TQueueExportProgress);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueExportProgress)

////////////////////////////////////////////////////////////////////////////////

struct TQueueExportProfilingCounters final
{
    //! Successfully exported row count.
    NProfiling::TCounter ExportedRows;
    //! Successfully exported chunk count.
    NProfiling::TCounter ExportedChunks;
    //! Successfully exported table count.
    NProfiling::TCounter ExportedTables;
    // Counter for the number of tables, which are skipped due to task error, but would otherwise be exported.
    NProfiling::TCounter SkippedTables;
    //! Counter for export task errors. It is incremented by 1 if by the end of task execution there was
    //! some error: task part error (e.g. failed to commit exported table subtransaction), or task error
    //! (e.g. failed to fetch queue chunk specs).
    NProfiling::TCounter ExportTaskErrors;
    //! Duration in seconds between the most recent exported table, which can be created,
    //! and actual last created exported table.
    NProfiling::TTimeGauge TimeLag;
    //! Number of exported tables, which can be created, but haven't been yet. It is derived
    //! from #TimeLag.
    NProfiling::TGauge TableLag;

    explicit TQueueExportProfilingCounters(const NProfiling::TProfiler& profiler);
};

using TQueueExportProfilingCountersPtr = TIntrusivePtr<TQueueExportProfilingCounters>;

////////////////////////////////////////////////////////////////////////////////

struct IQueueExporter
    : public virtual TRefCounted
{
    virtual TQueueExportProgressPtr GetExportProgress() const = 0;

    virtual void OnExportConfigChanged(const NQueueClient::TQueueStaticExportConfigPtr& newExportConfig) = 0;
    virtual void OnDynamicConfigChanged(const TQueueExporterDynamicConfig& newDynamicConfig) = 0;

    virtual void Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(IQueueExporter)

IQueueExporterPtr CreateQueueExporter(
    TString exportName,
    NQueueClient::TCrossClusterReference queue,
    NQueueClient::TQueueStaticExportConfigPtr exportConfig,
    TQueueExporterDynamicConfig dynamicConfig,
    NHiveClient::TClientDirectoryPtr clientDirectory,
    IInvokerPtr invoker,
    IQueueExportManagerPtr queueExportManager,
    NAlertManager::IAlertCollectorPtr alertCollector,
    const NProfiling::TProfiler& queueProfiler,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
