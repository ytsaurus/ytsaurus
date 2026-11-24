#include "sink.h"
#include "source.h"
#include "two_phase_commit_sink.h"
#include "ydb_topic_driver.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_RESOURCE(TYdbTopicDriver);
YT_FLOW_DEFINE_SOURCE(TYdbSource);
YT_FLOW_DEFINE_SOURCE(TYdbMultiLineSource);
YT_FLOW_DEFINE_SOURCE(TYdbFramingSource);
YT_FLOW_DEFINE_SINK(TYdbSink);
YT_FLOW_DEFINE_SINK(TYdbFramingSink);
YT_FLOW_DEFINE_SINK(TYdbTwoPhaseCommitSink);
YT_FLOW_DEFINE_SINK(TYdbAtLeastOnceSink);
YT_FLOW_DEFINE_SINK(TYdbAtLeastOnceFramingSink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
