#pragma once

#include "private.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TJobSizeAdjusterConfig
    : public NYTree::TYsonStruct
{
    TDuration MinJobTime;
    TDuration MaxJobTime;

    double ExecToPrepareTimeRatio;

    REGISTER_YSON_STRUCT(TJobSizeAdjusterConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
