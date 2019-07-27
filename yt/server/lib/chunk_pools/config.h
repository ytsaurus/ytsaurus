#pragma once

#include "private.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

class TJobSizeAdjusterConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MinJobTime;
    TDuration MaxJobTime;

    double ExecToPrepareTimeRatio;

    TJobSizeAdjusterConfig();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
