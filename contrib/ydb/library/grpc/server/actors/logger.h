#pragma once

#include <contrib/ydb/library/actors/core/log.h>
#include <contrib/ydb/library/grpc/server/logger.h>

namespace NYdbGrpc {

TLoggerPtr CreateActorSystemLogger(NActors::TActorSystem& as, NActors::NLog::EComponent component);

} // namespace NYdbGrpc
