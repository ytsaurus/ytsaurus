#pragma once

#include "private.h"

#include "config.h"
#include "queue_exporter.h"

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

class TQueueTabletExportProgressOld
    : public NYTree::TYsonStruct
{
public:
    NChunkClient::TChunkId LastChunk;
    NHiveClient::TTimestamp MaxTimestamp;
    i64 RowCount;
    i64 ChunkCount;

    REGISTER_YSON_STRUCT(TQueueTabletExportProgressOld);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueTabletExportProgressOld)

////////////////////////////////////////////////////////////////////////////////

class TQueueExportProgressOld
    : public NYTree::TYsonStruct
{
public:
    TInstant LastSuccessfulExportIterationInstant;
    TInstant LastExportedFramgentIterationInstant;
    ui64 LastExportedFragmentUnixTs;
    THashMap<i64, TQueueTabletExportProgressOldPtr> Tablets;
    //! Queue id corresponding to this export progress.
    NObjectClient::TObjectId QueueObjectId;

    void Update(i64 tabletIndex, NChunkClient::TChunkId chunkId, NHiveClient::TTimestamp maxTimestamp, i64 rowCount);

    REGISTER_YSON_STRUCT(TQueueExportProgressOld);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueExportProgressOld)

////////////////////////////////////////////////////////////////////////////////

struct TQueueExportProfilingCountersOld final
{
    NProfiling::TCounter ExportedRows;
    NProfiling::TCounter ExportedChunks;
    NProfiling::TCounter ExportedTables;

    explicit TQueueExportProfilingCountersOld(const NProfiling::TProfiler& profiler);
};

using TQueueExportProfilingCountersOldPtr = TIntrusivePtr<TQueueExportProfilingCountersOld>;

////////////////////////////////////////////////////////////////////////////////

class TQueueExporterOld
    : public IQueueExporter
{
public:
    TQueueExporterOld() = default;

    TQueueExporterOld(
        TString exportName,
        NQueueClient::TCrossClusterReference queue,
        const NQueueClient::TQueueStaticExportConfigPtr& exportConfig,
        const TQueueExporterDynamicConfig& dynamicConfig,
        NHiveClient::TClientDirectoryPtr clientDirectory,
        IInvokerPtr invoker,
        NAlertManager::IAlertCollectorPtr alertCollector,
        const NProfiling::TProfiler& queueProfiler,
        const NLogging::TLogger& logger);

    TQueueExportProgressOldPtr GetExportProgressOld() const;

    TQueueExportProgressPtr GetExportProgress() const override;

    void OnExportConfigChanged(const NQueueClient::TQueueStaticExportConfigPtr& newExportConfig) override;
    void OnDynamicConfigChanged(const TQueueExporterDynamicConfig& newDynamicConfig) override;

    void Stop() override;

    void BuildOrchidYson(NYTree::TFluentAny fluent) const override;

    EQueueExporterImplementation GetImplementationType() const override;

private:
    const TString ExportName_;
    const NQueueClient::TCrossClusterReference Queue_;

    NThreading::TSpinLock Lock_;
    NQueueClient::TQueueStaticExportConfigPtr ExportConfig_;
    TQueueExporterDynamicConfig DynamicConfig_;
    TQueueExportProgressOldPtr ExportProgress_;

    const NHiveClient::TClientDirectoryPtr ClientDirectory_;
    const IInvokerPtr Invoker_;
    const NAlertManager::IAlertCollectorPtr AlertCollector_;
    const TQueueExportProfilingCountersOldPtr ProfilingCounters_;

    NConcurrency::TScheduledExecutorPtr Executor_;

    const NLogging::TLogger Logger;

    void Export();
    void GuardedExport();

    NQueueClient::TQueueStaticExportConfigPtr GetConfig();
};

DEFINE_REFCOUNTED_TYPE(TQueueExporterOld)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
