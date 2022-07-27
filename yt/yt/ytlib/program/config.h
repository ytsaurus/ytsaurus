#pragma once

#include <yt/yt/library/program/config.h>

#include <yt/yt/ytlib/chunk_client/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNativeSingletonsConfig
    : public TSingletonsConfig
{
public:
    NChunkClient::TDispatcherConfigPtr ChunkClientDispatcher;

    REGISTER_YSON_STRUCT(TNativeSingletonsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNativeSingletonsConfig)

////////////////////////////////////////////////////////////////////////////////

class TNativeSingletonsDynamicConfig
    : public TSingletonsDynamicConfig
{
public:
    NChunkClient::TDispatcherDynamicConfigPtr ChunkClientDispatcher;

    REGISTER_YSON_STRUCT(TNativeSingletonsDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNativeSingletonsDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
