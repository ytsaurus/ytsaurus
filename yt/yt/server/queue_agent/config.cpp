#include "config.h"

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <yt/yt/library/re2/re2.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

void TQueueAgentConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("root", &TThis::Root);
}

////////////////////////////////////////////////////////////////////////////////

void TQueueAgentServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster_connection", &TThis::ClusterConnection);

    registrar.Parameter("queue_agent", &TThis::QueueAgent)
        .DefaultNew();
    registrar.Parameter("cypress_annotations", &TThis::CypressAnnotations)
        .Default(NYTree::BuildYsonNodeFluently()
            .BeginMap()
            .EndMap()
        ->AsMap());
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
