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
    TInstant LastSuccessfulExportIterationInstant;
    TInstant LastExportedFramgentIterationInstant;
    ui64 LastExportedFragmentUnixTs;
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
        NQueueClient::TCrossClusterReference queue,
        const NQueueClient::TQueueStaticExportConfig& exportConfig,
        const TQueueExporterDynamicConfig& dynamicConfig,
        NHiveClient::TClientDirectoryPtr clientDirectory,
        IInvokerPtr invoker,
        NAlertManager::IAlertCollectorPtr alertCollector,
        const NProfiling::TProfiler& queueProfiler,
        const NLogging::TLogger& logger);

    TQueueExportProgressPtr GetExportProgress() const;

    void Reconfigure(const NQueueClient::TQueueStaticExportConfig& newExportConfig, const TQueueExporterDynamicConfig& newDynamicConfig);

private:
    const TString ExportName_;
    const NQueueClient::TCrossClusterReference Queue_;

    NThreading::TSpinLock Lock_;
    NQueueClient::TQueueStaticExportConfig ExportConfig_;
    TQueueExporterDynamicConfig DynamicConfig_;
    TQueueExportProgressPtr ExportProgress_;

    const NHiveClient::TClientDirectoryPtr ClientDirectory_;
    const IInvokerPtr Invoker_;
    const NAlertManager::IAlertCollectorPtr AlertCollector_;
    const TQueueExportProfilingCountersPtr ProfilingCounters_;

    NConcurrency::TScheduledExecutorPtr Executor_;

    const NLogging::TLogger Logger;

    void Export();
    void GuardedExport();

    NQueueClient::TQueueStaticExportConfig GetConfig();
};

DEFINE_REFCOUNTED_TYPE(TQueueExporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
