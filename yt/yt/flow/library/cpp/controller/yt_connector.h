#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/yt_connector.h>

#include <yt/yt/core/actions/signal.h>
#include <yt/yt/core/ytree/public.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/prerequisite_client/public.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EYTConnectorState,
    (Disconnected)
    (Connecting)
    (Follower)
    (Leader)
);

struct IYTConnector
    : public ICommonYTConnector
{
    /*!
     *  \note Thread affinity: any
     */
    virtual void Start() = 0;

    /*!
     *  \note Thread affinity: any
     */
    virtual EYTConnectorState GetState() const = 0;

    /*!
     *  \note Thread affinity: any
     */
    virtual bool IsLeader() const = 0;

    //! Instant when this leader's address became discoverable by workers, or a null instant while
    //! it has not been published yet (or this node does not lead).
    /*!
     *  \note Thread affinity: any
     */
    virtual TInstant GetLeadershipPublishTime() const = 0;

    /*!
     *  \note Thread affinity: any
     */
    virtual void ValidateLeader() const = 0;

    /*!
     *  \note Thread affinity: any
     */
    virtual void Disconnect() = 0;

    //! Fences the transaction with the current leadership: the Cypress backend adds the
    //! leadership prerequisite id into prerequisites, the dyntable one rewrites the leader row
    //! inside the transaction so that a concurrent capture conflicts with this commit.
    virtual TFuture<NApi::ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        NApi::TTransactionStartOptions options = {}) = 0;

    virtual NPrerequisiteClient::TPrerequisiteId GetPrerequisiteId() const = 0;

    //! Tells the connector that the leader finished its recovery (the first scheduling iteration
    //! committed): with the dyntable backend this stops the recovery-time background renewal of
    //! the leader lease, leaving the fenced transactions as its only source. No-op otherwise.
    //! Idempotent.
    //!
    //! |leadershipEpoch| must be the value #GetLeadershipEpoch returned when the caller started
    //! leading: a call delayed across a demotion and a re-acquisition carries a stale epoch and
    //! is ignored instead of disarming the renewal of the leadership that is running now.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnLeaderRecoveryFinished(ui64 leadershipEpoch) = 0;

    //! Identifies the current leadership of the dyntable backend; always zero otherwise.
    /*!
     *  \note Thread affinity: any
     */
    virtual ui64 GetLeadershipEpoch() const = 0;

    //! Raised when connection became leader
    //! Subscribers may throw but cannot yield.
    DECLARE_INTERFACE_SIGNAL(void(), LeadingStarted);

    //! Raised when leading ends.
    //! Subscribers cannot neither throw nor yield.
    DECLARE_INTERFACE_SIGNAL(void(), LeadingEnded);
};

DEFINE_REFCOUNTED_TYPE(IYTConnector);

////////////////////////////////////////////////////////////////////////////////

IYTConnectorPtr CreateYTConnector(
    TControllerConfigPtr config,
    TNodeInfoPtr nodeInfo,
    ICommonYTConnectorPtr commonYTConnector,
    TControlActionQueuePtr controlQueue);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
