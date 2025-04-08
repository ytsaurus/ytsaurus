#pragma once

#include <yt/yt/server/lib/alert_manager/helpers.h>

#include <yt/yt/ytlib/queue_client/public.h>

#include <yt/yt/client/queue_client/common.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, QueueAgentLogger, "QueueAgent");
YT_DEFINE_GLOBAL(const NLogging::TLogger, QueueControllerLogger, "QueueController");
YT_DEFINE_GLOBAL(const NLogging::TLogger, ConsumerControllerLogger, "ConsumerController");
YT_DEFINE_GLOBAL(const NLogging::TLogger, QueueExporterLogger, "QueueExporter");
// COMPAT(apachee): For old queue export implementation.
YT_DEFINE_GLOBAL(const NLogging::TLogger, QueueStaticTableExporterLogger, "QueueStaticTableExporterLogger");
YT_DEFINE_GLOBAL(const NLogging::TLogger, QueueExportManagerLogger, "QueueExportManager");
YT_DEFINE_GLOBAL(const NLogging::TLogger, QueueAgentShardingManagerLogger, "QueueAgentShardingManager");
YT_DEFINE_GLOBAL(const NLogging::TLogger, CypressSynchronizerLogger, "CypressSynchronizer");

YT_DEFINE_GLOBAL(const NProfiling::TProfiler, QueueAgentProfiler, "/queue_agent");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, QueueAgentProfilerGlobal, QueueAgentProfiler().WithGlobal());

////////////////////////////////////////////////////////////////////////////////

namespace NAlerts {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((CypressSynchronizerUnableToFetchObjectRevisions)            (3000))
    ((CypressSynchronizerUnableToFetchAttributes)                 (3001))
    ((CypressSynchronizerPassFailed)                              (3002))

    ((QueueAgentPassFailed)                                       (3025))

    ((QueueAgentQueueControllerStaticExportFailed)                (3035))
    ((QueueAgentQueueControllerTrimFailed)                        (3036))
    ((QueueAgentQueueControllerStaticExportMisconfiguration)      (3037))

    ((QueueAgentShardingManagerPassFailed)                        (3050))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NAlerts

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TQueueAgent)
DECLARE_REFCOUNTED_CLASS(TQueueAgentConfig)
DECLARE_REFCOUNTED_CLASS(TQueueControllerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TQueueExportManagerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TQueueAgentDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(ICypressSynchronizer)
DECLARE_REFCOUNTED_CLASS(TCypressSynchronizer)
DECLARE_REFCOUNTED_CLASS(TCypressSynchronizerConfig)
DECLARE_REFCOUNTED_CLASS(TCypressSynchronizerDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(IQueueAgentShardingManager)
DECLARE_REFCOUNTED_CLASS(TQueueAgentShardingManagerDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TQueueAgentBootstrapConfig)
DECLARE_REFCOUNTED_CLASS(TQueueAgentProgramConfig)
DECLARE_REFCOUNTED_CLASS(TQueueAgentComponentDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

DECLARE_REFCOUNTED_STRUCT(IQueueExporter)
// COMPAT(apachee): For old queue export implementation.
DECLARE_REFCOUNTED_CLASS(TQueueExporterOld)

DECLARE_REFCOUNTED_STRUCT(IQueueExportManager)

////////////////////////////////////////////////////////////////////////////////

using TAgentId = TString;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IObjectStore)
DECLARE_REFCOUNTED_STRUCT(IObjectController)
DECLARE_REFCOUNTED_STRUCT(IQueueController)
DECLARE_REFCOUNTED_CLASS(TQueueAgentClientDirectory)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EObjectKind,
    (Queue)
    (Consumer)
);

DEFINE_ENUM(EQueueFamily,
    //! Sentinel value that does not correspond to any valid queue type.
    ((Null)                       (0))
    //! Regular ordered dynamic table.
    ((OrderedDynamicTable)        (1))
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TQueueSnapshot)
using TQueueSnapshotConstPtr = TIntrusivePtr<const TQueueSnapshot>;
DECLARE_REFCOUNTED_STRUCT(TQueuePartitionSnapshot)
DECLARE_REFCOUNTED_STRUCT(TConsumerSnapshot)
DECLARE_REFCOUNTED_STRUCT(TSubConsumerSnapshot)
DECLARE_REFCOUNTED_STRUCT(TConsumerPartitionSnapshot)
using TConsumerSnapshotConstPtr = TIntrusivePtr<const TConsumerSnapshot>;
using TSubConsumerSnapshotConstPtr = TIntrusivePtr<const TSubConsumerSnapshot>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IQueueProfileManager)
DECLARE_REFCOUNTED_STRUCT(IConsumerProfileManager)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EConsumerPartitionDisposition,
    //! Sentinel value.
    (None)
    //! At the end of the window, i.e. unread row count == 0.
    (UpToDate)
    //! Inside the window but not at the end, i.e. 0 < unread row count <= available row count.
    (PendingConsumption)
    //! Past the window, i.e. unread row count > available row count.
    (Expired)
    //! Ahead of the window, i.e. "unread row count < 0" (unread row count is capped)
    (Ahead)
);

////////////////////////////////////////////////////////////////////////////////

inline const std::string NoneQueueAgentStage = "none";
inline const TString NoneObjectType = "none";

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IBootstrap)

DECLARE_REFCOUNTED_CLASS(TQueueTabletExportProgress)
DECLARE_REFCOUNTED_CLASS(TQueueExportProgress)
// COMPAT(apachee): For old queue export implementation.
DECLARE_REFCOUNTED_CLASS(TQueueTabletExportProgressOld)
DECLARE_REFCOUNTED_CLASS(TQueueExportProgressOld)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
