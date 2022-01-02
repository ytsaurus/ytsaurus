#pragma once

#include "private.h"

#include "state.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

//! Object responsible for tracking the list of queues assigned to this particular controller.
class TQueueAgent
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NYTree::IYPathServicePtr, OrchidService);

public:
    TQueueAgent(
        TQueueAgentConfigPtr config,
        IInvokerPtr controlInvoker,
        NApi::NNative::IClientPtr client,
        TAgentId agentId);

    void Start();

private:
    const TQueueAgentConfigPtr Config_;
    const IInvokerPtr ControlInvoker_;
    const NConcurrency::TThreadPoolPtr ControllerThreadPool_;
    const NApi::NNative::IClientPtr Client_;
    const TAgentId AgentId_;
    const NConcurrency::TPeriodicExecutorPtr PollExecutor_;

    const TQueueTablePtr QueueTable_;

    struct TQueue
    {
        //! If set, defines the reason why this queue is not functioning properly.
        TError Error;
        //! Non-null iff Error.IsOK().
        IQueueControllerPtr Controller;
        //! Revision of a queue row, for which the #Controller is created.
        NHydra::TRevision Revision = NHydra::NullRevision;
    };

    using TQueueMap = THashMap<TQueueId, TQueue>;
    TQueueMap Queues_;

    //! Latest non-trivial poll iteration error.
    TError LatestPollError_ = TError() << TErrorAttribute("poll_index", -1);
    //! Current poll iteration instant.
    TInstant PollInstant_ = TInstant::Zero();
    //! Index of a current poll iteration.
    i64 PollIndex_ = 0;

    void BuildOrchid(NYson::IYsonConsumer* consumer) const;

    //! Creates orchid node at the native cluster.
    void UpdateOrchidNode();

    //! One iteration of state polling and queue/consumer in-memory state updating.
    void Poll();

    //! Called for each polled queue row from state table.
    void ProcessQueueRow(const TQueueTableRow& row);
    //! Called for each queue that does not have a corresponding row in the state table.
    void UnregisterQueue(const TQueueId& queue);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgent)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
