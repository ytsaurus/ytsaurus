#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

class TDynamicSequoiaManagerConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;

    bool FetchChunkMetaFromSequoia;

    REGISTER_YSON_STRUCT(TDynamicSequoiaManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSequoiaManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
