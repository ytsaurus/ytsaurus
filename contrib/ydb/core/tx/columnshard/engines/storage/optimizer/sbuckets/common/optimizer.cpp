#include "optimizer.h"
#include <contrib/ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets {

TDuration GetCommonFreshnessCheckDuration() {
    return NYDBTest::TControllers::GetColumnShardController()->GetOptimizerFreshnessCheckDuration();
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets
