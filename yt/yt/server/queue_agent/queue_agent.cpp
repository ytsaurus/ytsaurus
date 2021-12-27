#include "queue_agent.h"

#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NQueueAgent {

using namespace NYTree;
using namespace NObjectClient;
using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const auto& Logger = QueueAgentLogger;

TQueueAgent::TQueueAgent(
    TQueueAgentConfigPtr config,
    IInvokerPtr controlInvoker,
    NApi::NNative::IClientPtr client,
    TAgentId agentId)
    : Config_(std::move(config))
    , ControlInvoker_(std::move(controlInvoker))
    , Client_(std::move(client))
    , AgentId_(std::move(agentId))
{
    UpdateOrchidNode();
}

IYPathServicePtr TQueueAgent::GetOrchid() const
{
    return BuildYsonNodeFluently()
        .BeginMap()
        .EndMap();
}

void TQueueAgent::UpdateOrchidNode()
{
    TCreateNodeOptions options;
    options.Force = true;
    options.Recursive = true;
    options.Attributes = ConvertToAttributes(
        BuildYsonStringFluently().BeginMap()
            .Item("remote_addresses").BeginMap()
                .Item("default").Value(AgentId_)
            .EndMap()
        .EndMap());

    auto orchidPath = Format("%v/instances/%v", Config_->Root, AgentId_);

    WaitFor(Client_->CreateNode(orchidPath, EObjectType::Orchid, options))
        .ThrowOnError();

    YT_LOG_INFO("Orchid node updated");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
