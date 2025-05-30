#include "background_controller.h"

#include <contrib/ydb/core/base/appdata_fwd.h>
#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr::NColumnShard {

void TBackgroundControllerCounters::OnCompactionFinish(TInternalPathId pathId) {
    TInstant now = TAppData::TimeProvider->Now();
    TInstant& lastFinish = LastCompactionFinishByPathId[pathId];
    lastFinish = std::max(lastFinish, now);

    if (LastCompactionFinish < now) {
        LastCompactionFinish = now;
    }
}

} // namespace NKikimr::NColumnShard
