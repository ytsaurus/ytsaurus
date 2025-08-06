#include "public.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

const std::string CommittedAttribute("committed");

////////////////////////////////////////////////////////////////////////////////

const std::string DefaultTreeAttributeName("default_tree");
const std::string TreeConfigAttributeName("config");
const std::string IdAttributeName("id");
const std::string ParentIdAttributeName("parent_id");

const NYPath::TYPath StrategyStatePath("//sys/scheduler/strategy_state");
const NYPath::TYPath OldSegmentsStatePath("//sys/scheduler/segments_state");
const NYPath::TYPath LastMeteringLogTimePath("//sys/scheduler/@last_metering_log_time");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
