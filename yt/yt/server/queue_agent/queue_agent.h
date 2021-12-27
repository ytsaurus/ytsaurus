#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

//! Object responsible for tracking the list of queues assigned to this particular controller.
class TQueueAgent
    : public TRefCounted
{
public:
    TQueueAgent(
        TQueueAgentConfigPtr config,
        IInvokerPtr controlInvoker,
        NApi::NNative::IClientPtr client,
        TAgentId agentId);

    NYTree::IYPathServicePtr GetOrchid() const;

private:
    TQueueAgentConfigPtr Config_;
    IInvokerPtr ControlInvoker_;
    NApi::NNative::IClientPtr Client_;
    TAgentId AgentId_;

    //! Creates orchid node at the native cluster.
    void UpdateOrchidNode();
};

DEFINE_REFCOUNTED_TYPE(TQueueAgent)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
