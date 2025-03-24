#pragma once

#include <contrib/ydb/core/util/metrics.h>
#include "hive_schema.h"

namespace NKikimr {
namespace NHive {

using TMetricsMaximum = NMetrics::TMaximumValueVariableWindowUI64;

struct TTabletMetricsAggregates {
    TMetricsMaximum MaximumCPU;
    TMetricsMaximum MaximumMemory;
    TMetricsMaximum MaximumNetwork;
};

}
}
