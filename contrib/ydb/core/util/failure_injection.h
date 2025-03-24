#pragma once

#include "defs.h"
#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/library/actors/core/actor.h>

namespace NKikimr {

    NActors::IActor *CreateFailureInjectionActor(const NKikimrConfig::TFailureInjectionConfig& config, const NKikimr::TAppData& appData);

} // NKikimr
