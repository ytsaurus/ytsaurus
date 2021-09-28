#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

class TIOTrackerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! If set to true, logging of IO events is enabled.
    bool Enable;

    //! If set to true, raw IO events can be logged. Otherwise, only aggregated events are logged.
    bool EnableRaw;

    //! Queue size for IO events that were enqueued but were not logged. If the queue size exceeds
    //! its limit, incoming events will be dropped.
    int QueueSizeLimit;

    //! Number of aggregated IO events kept in memory. The events which don't fit into this limit
    //! are dropped.
    int AggregationSizeLimit;

    //! Period during which the events are aggregated. When the period is finished, all the aggregated
    //! events are flushed into the log.
    TDuration AggregationPeriod;

    //! Period used to poll the queue for new events.
    TDuration PeriodQuant;

    TIOTrackerConfig();
};

DEFINE_REFCOUNTED_TYPE(TIOTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
