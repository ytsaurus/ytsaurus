#include "public.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

const TString CommittedAttribute("committed");

////////////////////////////////////////////////////////////////////////////////

const TString DefaultTreeAttributeName("default_tree");
const TString TreeConfigAttributeName("config");
const TString IdAttributeName("id");
const TString StrategyStatePath("//sys/scheduler/strategy_state");
const TString OldSegmentsStatePath("//sys/scheduler/segments_state");
const TString LastMeteringLogTimePath("//sys/scheduler/@last_metering_log_time");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
