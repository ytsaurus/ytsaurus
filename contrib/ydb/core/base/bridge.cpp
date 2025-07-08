#include "bridge.h"

#include <contrib/ydb/core/protos/config.pb.h>

namespace NKikimr {

bool IsBridgeMode(const TActorContext &ctx) {
    const auto *bridgeConfig = AppData(ctx)->BridgeConfig;
    return bridgeConfig && bridgeConfig->PilesSize() > 0;
}

} // NKikimr
