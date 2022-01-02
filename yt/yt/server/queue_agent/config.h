#pragma once

#include "private.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

class TQueueAgentConfig
    : public NYTree::TYsonStruct
{
public:
    //! The path of directory containing queue agent state.
    NYPath::TYPath Root;

    //! State table poll period.
    TDuration PollPeriod;

    //! Controller thread pool thread count.
    int ControllerThreadCount;

    REGISTER_YSON_STRUCT(TQueueAgentConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentConfig)

////////////////////////////////////////////////////////////////////////////////

class TQueueAgentServerConfig
    : public TServerConfig
{
public:
    TQueueAgentConfigPtr QueueAgent;

    NApi::NNative::TConnectionConfigPtr ClusterConnection;

    NYTree::IMapNodePtr CypressAnnotations;

    bool AbortOnUnrecognizedOptions;

    //! User for native client; defaults to queue-agent.
    TString User;

    REGISTER_YSON_STRUCT(TQueueAgentServerConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
