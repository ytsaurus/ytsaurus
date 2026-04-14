#pragma once

#include <contrib/ydb/core/base/defs.h>
#include <contrib/ydb/library/actors/core/event_local.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <contrib/ydb/library/services/services.pb.h>

namespace NKikimr::NDataShardLoad {

using TUploadRequest = std::unique_ptr<IEventBase>;
using TRequestsVector = std::vector<TUploadRequest>;

} // NKikimr::NDataShardLoad
