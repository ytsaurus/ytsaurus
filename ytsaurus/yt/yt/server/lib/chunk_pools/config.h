#pragma once

#include "private.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

class TJobSizeAdjusterConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration MinJobTime;
    TDuration MaxJobTime;

    double ExecToPrepareTimeRatio;

    REGISTER_YSON_STRUCT(TJobSizeAdjusterConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
