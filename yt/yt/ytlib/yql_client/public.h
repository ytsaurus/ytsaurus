#pragma once

#include <yt/yt/core/misc/common.h>

#include <library/cpp/yt/error/error_code.h>

namespace NYT::NYqlClient {

////////////////////////////////////////////////////////////////////////////////

//! Counterpart of NYql::EExecuteMode.
DEFINE_ENUM(EExecuteMode,
    ((Validate)    (0))
    ((Optimize)    (1))
    ((Run)         (2))
);

DEFINE_ENUM(EQueryType,
    ((Regular)    (0))
    ((UdfMeta)    (1))
);

YT_DEFINE_ERROR_ENUM(
    ((RequestThrottled)     (40100))
    ((YqlAgentBanned)       (40101))
    ((YqlAgentNotReady)     (40102))
);

DEFINE_STRING_SERIALIZABLE_ENUM(EProgressPart,
    ((YqlPlan)       (0))
    ((YqlStatistics) (1))
    ((YqlProgress)   (2))
    ((YqlTaskInfo)   (3))
    ((YqlAst)        (4))
    ((YqlRevision)   (5))
);

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

class TYqlResponse;

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TYqlAgentChannelConfig)
DECLARE_REFCOUNTED_STRUCT(TYqlAgentStageConfig)
DECLARE_REFCOUNTED_STRUCT(TYqlAgentConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlClient
