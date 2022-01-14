#pragma once

#include "private.h"

#include <yt/yt/server/lib/cypress_election/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

class TQueueControllerConfig
    : public NYTree::TYsonStruct
{
public:
    //! Loop period. Defines the period of monitoring information exporting, automatic
    //! trimming and the maximum age of cached Orchid state.
    //!
    //! Null stands for "no internal update loop"; in this mode no information is
    //! exported to monitoring and automatic trimming does not work. Orchid read requests
    //! are still handled synchronously as opposite to being served from cache when
    //! #LoopPeriod is non-null.
    std::optional<TDuration> LoopPeriod;

    REGISTER_YSON_STRUCT(TQueueControllerConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueControllerConfig)

////////////////////////////////////////////////////////////////////////////////

class TQueueAgentConfig
    : public NYTree::TYsonStruct
{
public:
    //! State table poll period.
    TDuration PollPeriod;

    //! Controller thread pool thread count.
    int ControllerThreadCount;

    //! Configuration of queue controllers.
    TQueueControllerConfigPtr Controller;

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

    bool AbortOnUnrecognizedOptions;

    //! User for native client; defaults to queue-agent.
    TString User;

    //! The path of directory containing queue agent state.
    NYPath::TYPath Root;

    NYTree::IMapNodePtr CypressAnnotations;

    NCypressElection::TCypressElectionManagerConfigPtr ElectionManager;

    REGISTER_YSON_STRUCT(TQueueAgentServerConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
