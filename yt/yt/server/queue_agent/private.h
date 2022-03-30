 #pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger QueueAgentLogger("QueueAgent");
inline const NLogging::TLogger CypressSynchronizerLogger("CypressSynchronizer");
inline const NProfiling::TProfiler QueueAgentProfiler("/queue_agent");

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TQueueControllerConfig)
DECLARE_REFCOUNTED_CLASS(TQueueAgent)
DECLARE_REFCOUNTED_CLASS(TQueueAgentConfig)
DECLARE_REFCOUNTED_CLASS(TQueueAgentServerConfig)

DECLARE_REFCOUNTED_STRUCT(ICypressSynchronizer)
DECLARE_REFCOUNTED_CLASS(TPollingCypressSynchronizer)
DECLARE_REFCOUNTED_CLASS(TCypressSynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

using TAgentId = TString;

////////////////////////////////////////////////////////////////////////////////

using TRowRevision = ui64;
constexpr TRowRevision NullRowRevision = 0;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TQueueTable)
DECLARE_REFCOUNTED_CLASS(TConsumerTable)
DECLARE_REFCOUNTED_STRUCT(TDynamicState)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IQueueController)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EQueueFamily,
    //! Sentinel value that does not correspond to any valid queue type.
    ((Null)                       (0))
    //! Regular ordered dynamic table.
    ((OrderedDynamicTable)        (1))
)

////////////////////////////////////////////////////////////////////////////////

struct TCrossClusterReference;
struct TQueueTableRow;
struct TConsumerTableRow;

using TConsumerRowMap = THashMap<TCrossClusterReference, TConsumerTableRow>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TQueueSnapshot);
DECLARE_REFCOUNTED_STRUCT(TQueuePartitionSnapshot);
DECLARE_REFCOUNTED_STRUCT(TConsumerSnapshot);
DECLARE_REFCOUNTED_STRUCT(TConsumerPartitionSnapshot);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IConsumerTable);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IQueueProfileManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
