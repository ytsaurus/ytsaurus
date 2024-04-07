#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

class TDynamicSequoiaQueueConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration FlushPeriod;

    int FlushBatchSize;

    bool PauseFlush;

    REGISTER_YSON_STRUCT(TDynamicSequoiaQueueConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSequoiaQueueConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicSequoiaManagerConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;

    // TODO(kvk1920): unused option?
    bool FetchChunkMetaFromSequoia;

    TDynamicSequoiaQueueConfigPtr SequoiaQueue;

    bool EnableCypressTransactionsInSequoia;

    REGISTER_YSON_STRUCT(TDynamicSequoiaManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSequoiaManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
