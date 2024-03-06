#pragma once

#include <yt/yt/server/lib/alert_manager/helpers.h>

#include <yt/yt/ytlib/queue_client/public.h>

#include <yt/yt/client/queue_client/common.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger QueueAgentLogger("QueueAgent");
inline const NLogging::TLogger QueueAgentShardingManagerLogger("QueueAgentShardingManager");
inline const NLogging::TLogger CypressSynchronizerLogger("CypressSynchronizer");
inline const NProfiling::TProfiler QueueAgentProfilerGlobal = NProfiling::TProfiler("/queue_agent").WithGlobal();
inline const NProfiling::TProfiler QueueAgentProfiler = NProfiling::TProfiler("/queue_agent");

////////////////////////////////////////////////////////////////////////////////

namespace NAlerts {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((CypressSynchronizerUnableToFetchObjectRevisions)            (3000))
    ((CypressSynchronizerUnableToFetchAttributes)                 (3001))
    ((CypressSynchronizerPassFailed)                              (3002))

    ((QueueAgentPassFailed)                                       (3025))

    ((QueueAgentShardingManagerPassFailed)                        (3050))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NAlerts

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TQueueAgent)
DECLARE_REFCOUNTED_CLASS(TQueueAgentConfig)
DECLARE_REFCOUNTED_CLASS(TQueueControllerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TQueueAgentDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(ICypressSynchronizer)
DECLARE_REFCOUNTED_CLASS(TCypressSynchronizer)
DECLARE_REFCOUNTED_CLASS(TCypressSynchronizerConfig)
DECLARE_REFCOUNTED_CLASS(TCypressSynchronizerDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(IQueueAgentShardingManager)
DECLARE_REFCOUNTED_CLASS(TQueueAgentShardingManagerDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TQueueAgentServerConfig)
DECLARE_REFCOUNTED_CLASS(TQueueAgentServerDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

DECLARE_REFCOUNTED_CLASS(TQueueExporter)

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

inline const TString NoneQueueAgentStage = "none";
inline const TString NoneObjectType = "none";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
