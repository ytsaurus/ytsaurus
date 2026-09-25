#include "private.h"

#include "kafka_client.h"
#include "sink.h"
#include "source.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_RESOURCE(TKafkaClient);
YT_FLOW_DEFINE_SOURCE(TKafkaSource);
YT_FLOW_DEFINE_SINK(TKafkaSink);
YT_FLOW_DEFINE_SINK(TAtLeastOnceKafkaSink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
