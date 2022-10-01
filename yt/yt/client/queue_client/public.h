#pragma once

#include <yt/yt/core/misc/common.h>
#include <yt/yt/core/misc/error_code.h>
#include <yt/yt/core/misc/ref_counted.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((ConsumerOffsetConflict)            (3100))
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EQueueAutoTrimPolicy,
    //! No automatic trimming is performed.
    (None)
    //! The queue is automatically trimmed up to the smallest offset among vital consumers.
    //! If no vital consumers exist for a queue, nothing is trimmed, yet using this mode would be a misconfiguration.
    (VitalConsumers)
)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IQueueRowset)

DECLARE_REFCOUNTED_STRUCT(IPersistentQueueRowset)

DECLARE_REFCOUNTED_STRUCT(IConsumerClient)

DECLARE_REFCOUNTED_STRUCT(IPartitionReader)
DECLARE_REFCOUNTED_CLASS(TPartitionReaderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
