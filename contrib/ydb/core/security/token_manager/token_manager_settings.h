#pragma once

#include <contrib/ydb/core/protos/auth.pb.h>
#include <contrib/ydb/library/actors/core/actorid.h>

#include <optional>

namespace NKikimr {

struct TTokenManagerSettings {
    NKikimrProto::TTokenManager Config;
    std::optional<NActors::TActorId> HttpProxyId;
};

} // NKikimr
