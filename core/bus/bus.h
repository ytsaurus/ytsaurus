#pragma once

#include "public.h"

#include <yt/core/actions/future.h>
#include <yt/core/actions/signal.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/error.h>

#include <yt/core/ytree/public.h>

#include <yt/core/net/public.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct TTcpDispatcherStatistics
{
    i64 InBytes = 0;
    i64 InPackets = 0;

    i64 OutBytes = 0;
    i64 OutPackets = 0;

    i64 PendingOutPackets = 0;
    i64 PendingOutBytes = 0;

    int ClientConnections = 0;
    int ServerConnections = 0;

    i64 StalledReads = 0;
    i64 StalledWrites = 0;

    i64 ReadErrors = 0;
    i64 WriteErrors = 0;

    i64 EncoderErrors = 0;
    i64 DecoderErrors = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TSendOptions
{
    static constexpr int AllParts = -1;

    explicit TSendOptions(
        EDeliveryTrackingLevel trackingLevel = EDeliveryTrackingLevel::None,
        int checksummedPartCount = AllParts)
        : TrackingLevel(trackingLevel)
        , ChecksummedPartCount(checksummedPartCount)
    { }

    EDeliveryTrackingLevel TrackingLevel;
    int ChecksummedPartCount;
};

//! A bus, i.e. something capable of transmitting messages.
struct IBus
    : public virtual TRefCounted
{
    //! Returns a textual representation of the bus' endpoint.
    //! Typically used for logging.
    virtual const TString& GetEndpointDescription() const = 0;

    //! Returns the bus' endpoint attributes.
    //! Typically used for constructing errors.
    virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const = 0;

    virtual const NNet::TNetworkAddress& GetEndpointAddress() const = 0;

    virtual TTcpDispatcherStatistics GetStatistics() const = 0;

    //! Asynchronously sends a message via the bus.
    /*!
     *  \param message A message to send.
     *  \return An asynchronous flag indicating if the delivery (not the processing!) of the message
     *  was successful.
     *
     *  \note Thread affinity: any
     */
    virtual TFuture<void> Send(TSharedRefArray message, const TSendOptions& options) = 0;

    //! For socket buses, updates the TOS level.
    /*!
     *  \note Thread affinity: any
     */
    virtual void SetTosLevel(TTosLevel tosLevel) = 0;

    //! Terminates the bus.
    /*!
     *  Does not block -- termination typically happens in background.
     *  It is safe to call this method multiple times.
     *  On terminated the instance is no longer usable.

     *  \note Thread affinity: any.
     */
    virtual void Terminate(const TError& error) = 0;

    //! Invoked upon bus termination
    //! (either due to call to #Terminate or other party's failure).
    DECLARE_INTERFACE_SIGNAL(void(const TError&), Terminated);
};

DEFINE_REFCOUNTED_TYPE(IBus)

////////////////////////////////////////////////////////////////////////////////

//! Handles incoming bus messages.
struct IMessageHandler
    : public virtual TRefCounted
{
    //! Raised whenever a new message arrives via the bus.
    /*!
     *  \param message The just arrived message.
     *  \param replyBus A bus that can be used for replying back.
     *
     *  \note
     *  Thread affinity: this method is called from an unspecified thread
     *  and must return ASAP. No context switch or fiber cancelation is possible.
     *
     */
    virtual void HandleMessage(TSharedRefArray message, IBusPtr replyBus) throw() = 0;
};

DEFINE_REFCOUNTED_TYPE(IMessageHandler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
