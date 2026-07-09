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

    /*!
     *  \note Thread affinity: any
     */
    virtual void ValidateLeader() const = 0;

    /*!
     *  \note Thread affinity: any
     */
    virtual void Disconnect() = 0;

    //! Adds the current leadership prerequisite id into prerequisites.
    virtual TFuture<NApi::ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        NApi::TTransactionStartOptions options = {}) = 0;

    virtual NPrerequisiteClient::TPrerequisiteId GetPrerequisiteId() const = 0;

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
