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
    //! Instant corresponding to the last export task, which had no errors.
    TInstant LastSuccessfulExportTaskInstant;
    // COMPAT(apachee): Duplicate of LastSuccessfulExportTaskInstant, used for compatability.
    TInstant LastSuccessfulExportIterationInstant;
    //! Instant corresponding to the last export task.
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
    NProfiling::TCounter ExportedRows;
    NProfiling::TCounter ExportedChunks;
    NProfiling::TCounter ExportedTables;
    NProfiling::TCounter SkippedTables;
    NProfiling::TCounter ExportTaskErrors;

    explicit TQueueExportProfilingCounters(const NProfiling::TProfiler& profiler);
};

using TQueueExportProfilingCountersPtr = TIntrusivePtr<TQueueExportProfilingCounters>;

////////////////////////////////////////////////////////////////////////////////

struct IQueueStaticTableExporter
    : public virtual TRefCounted
{
    virtual TQueueExportProgressPtr GetExportProgress() const = 0;

    virtual void OnExportConfigChanged(const NQueueClient::TQueueStaticExportConfigPtr& newExportConfig) = 0;
    virtual void OnDynamicConfigChanged(const TQueueExporterDynamicConfig& newDynamicConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IQueueStaticTableExporter)

IQueueStaticTableExporterPtr CreateQueueStaticTableExporter(
    TString exportName,
    NQueueClient::TCrossClusterReference queue,
    NQueueClient::TQueueStaticExportConfigPtr exportConfig,
    TQueueExporterDynamicConfig dynamicConfig,
    NHiveClient::TClientDirectoryPtr clientDirectory,
    IInvokerPtr invoker,
    IQueueStaticTableExportManagerPtr queueStaticTableExportManager,
    NAlertManager::IAlertCollectorPtr alertCollector,
    const NProfiling::TProfiler& queueProfiler,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
