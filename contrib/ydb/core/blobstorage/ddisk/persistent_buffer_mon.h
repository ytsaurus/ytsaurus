#pragma once

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/library/actors/core/actor.h>

namespace NKikimr {

    NActors::IActor *CreateMonPersistentBufferActor(const NKikimrConfig::TAppConfig& config, const NKikimr::TAppData& appData);

} // NKikimr
