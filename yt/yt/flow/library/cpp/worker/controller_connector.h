#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/actions/signal.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

// XXX(babenko): some methods are unused.
/*!
 *  \note Thread affinity: single-threaded unless noted otherwise
 */
struct IControllerConnector
    : public TRefCounted
{
    virtual void Initialize() = 0;

    /*!
     *  \note Thread affinity: any
     */
    virtual NYTree::IYPathServicePtr CreateOrchidService() = 0;
    /*!
     *  \note Thread affinity: any
     */
    virtual TIncarnationId GetIncarnationId() const = 0;
    /*!
     *  \note Thread affinity: any
     */
    virtual void Disconnect(const TError& error) = 0;

    //! Returns the RPC channel to the current controller leader, or null if
    //! the connector is currently disconnected. Reconnects (incl. on failover)
    //! transparently update the returned channel on subsequent calls.
    /*!
     *  \note Thread affinity: any
     */
    virtual NRpc::IChannelPtr GetControllerChannel() const = 0;

    //! Raised when connection process starts.
    //! Subscribers may throw and yield.
    DECLARE_INTERFACE_SIGNAL(void(), ControllerConnecting);

    //! Raised when connection is complete.
    //! Subscribers may throw but cannot yield.
    DECLARE_INTERFACE_SIGNAL(void(TIncarnationId), ControllerConnected);

    //! Raised when disconnect happens.
    //! Subscribers cannot neither throw nor yield.
    DECLARE_INTERFACE_SIGNAL(void(), ControllerDisconnected);
};

DEFINE_REFCOUNTED_TYPE(IControllerConnector);

////////////////////////////////////////////////////////////////////////////////

IControllerConnectorPtr CreateControllerConnector(
    TNodeInfoPtr nodeInfo,
    std::vector<std::string> groups,
    THashMap<std::string, ssize_t> capabilities,
    ICommonYTConnectorPtr ytConnector,
    IInvokerPtr controlInvoker,
    NRpc::IChannelFactoryPtr channelFactory,
    IMessageDistributorPtr messageDistributor,
    IJobDirectoryPtr jobDirectory,
    IJobTrackerPtr jobTracker,
    TStreamSpecStoragePtr streamSpecStorage,
    bool ignoreSingletonsDynamicConfig,
    IStatusProfilerPtr rootStatusProfiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
