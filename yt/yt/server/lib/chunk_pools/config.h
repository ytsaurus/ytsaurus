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

    // TODO(coteeq): When(if) map operation will be properly split for
    // ordered and unordered versions, this field could live in its own config.
    //! A step for discrete version.
    double DataWeightFactor;

    REGISTER_YSON_STRUCT(TJobSizeAdjusterConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
