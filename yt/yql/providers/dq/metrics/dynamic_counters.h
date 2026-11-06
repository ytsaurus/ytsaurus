#pragma once

#include <yt/yt/library/profiling/producer.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYql {

// Counter values retain their derivative/gauge semantics. Group labels become tags;
// repeated label keys get _2, _3, ... suffixes. Non-standard counter label keys are
// preserved in counter_label. Histograms are exported as non-cumulative /bucket
// series with an upper_bound tag (including +Inf), since ISensorWriter has no
// histogram API. Register with WithProducerRemoveSupport() to retire removed groups.
NYT::NProfiling::ISensorProducerPtr CreateDynamicCountersProducer(
    NMonitoring::TDynamicCounterPtr counters);

} // namespace NYql
