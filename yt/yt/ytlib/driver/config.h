#pragma once

#include "public.h"

#include <yt/yt/library/auth_server/config.h>

#include <yt/yt/client/driver/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/ytlib/chunk_client/public.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TNativeDriverConfig
    : public TDriverConfig
{
    NAuth::TTvmServiceConfigPtr TvmService;
    NChunkClient::TDispatcherConfigPtr ChunkClientDispatcher;
    bool StartQueueConsumerRegistrationManager;

    REGISTER_YSON_STRUCT(TNativeDriverConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNativeDriverConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
