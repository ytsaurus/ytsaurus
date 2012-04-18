#include "stdafx.h"
#include "roaming_channel.h"
#include "client.h"

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

class TResponseHandlerWrapper
    : public IClientResponseHandler
{
public:
    typedef TIntrusivePtr<TResponseHandlerWrapper> TPtr;

    TResponseHandlerWrapper(
        IClientResponseHandler::TPtr underlyingHandler,
        TClosure onFailed)
        : UnderlyingHandler(underlyingHandler)
        , OnFailed(onFailed)
    { }

    virtual void OnAcknowledgement()
    {
        UnderlyingHandler->OnAcknowledgement();
    }

    virtual void OnResponse(IMessage* message)
    {
        UnderlyingHandler->OnResponse(message);
    }

    virtual void OnError(const TError& error)
    {
        UnderlyingHandler->OnError(error);

        auto code = error.GetCode();
        if (code == EErrorCode::Timeout ||
            code == EErrorCode::TransportError ||
            code == EErrorCode::Unavailable)
        {
            OnFailed.Run();
        }
    }

private:
    IClientResponseHandler::TPtr UnderlyingHandler;
    TClosure OnFailed;

};

////////////////////////////////////////////////////////////////////////////////

class TRoamingChannel
    : public IChannel
{
public:
    TRoamingChannel(
        TNullable<TDuration> defaultTimeout,
        TChannelProducer producer)
        : DefaultTimeout(defaultTimeout)
        , Producer(producer)
        , ChannelPromise(Null)
    { }

    virtual TNullable<TDuration> GetDefaultTimeout() const
    {
        return DefaultTimeout;
    }

    virtual void Send(
        IClientRequest* request,
        IClientResponseHandler* responseHandler,
        TNullable<TDuration> timeout)
    {
        YASSERT(request);
        YASSERT(responseHandler);

        GetChannel().Subscribe(BIND(
            &TRoamingChannel::OnGotChannel,
            MakeStrong(this),
            MakeStrong(request),
            MakeStrong(responseHandler),
            timeout));
    }

    virtual void Terminate()
    {
        TGuard<TSpinLock> guard(SpinLock);

        // TODO(babenko): this does not look correct
        // but we should get rid of Terminate soon anyway.
    
        auto currentChannel = ChannelPromise.TryGet();
        if (currentChannel && currentChannel->IsOK()) {
            currentChannel->Value()->Terminate();
        }

        ChannelPromise.Reset();
    }

private:
    friend class TResponseHandlerWrapper;

    TFuture< TValueOrError<IChannel::TPtr> > GetChannel()
    {
        TGuard<TSpinLock> guard(SpinLock);
        
        if (!ChannelPromise.IsNull()) {
            return ChannelPromise;
        }

        auto promisedChannel = ChannelPromise = NewPromise< TValueOrError<IChannel::TPtr> >();
        guard.Release();

        Producer.Run().Subscribe(BIND(
            &TRoamingChannel::OnEndpointDiscovered,
            MakeStrong(this),
            promisedChannel));
        return promisedChannel;
    }

    void OnEndpointDiscovered(
        TPromise< TValueOrError<IChannel::TPtr> > channelPromise,
        TValueOrError<IChannel::TPtr> result)
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (ChannelPromise == channelPromise) {
            channelPromise.Set(result);
            if (!result.IsOK()) {
                ChannelPromise.Reset();
            }
        }
    }
         
    void OnGotChannel(
        IClientRequest::TPtr request,
        IClientResponseHandler::TPtr responseHandler,
        TNullable<TDuration> timeout,
        TValueOrError<IChannel::TPtr> result)
    {
        if (!result.IsOK()) {
            responseHandler->OnError(result);
        } else {
            auto channel = result.Value();
            auto responseHandlerWrapper = New<TResponseHandlerWrapper>(
                ~responseHandler,
                BIND(&TRoamingChannel::OnChannelFailed, MakeStrong(this), channel));
            channel->Send(~request, ~responseHandlerWrapper, timeout);
        }
    }

    void OnChannelFailed(IChannel::TPtr failedChannel)
    {
        TGuard<TSpinLock> guard(SpinLock);

        if (!ChannelPromise.IsNull()) {
            auto currentChannel = ChannelPromise.TryGet();
            if (
                currentChannel && currentChannel->IsOK() &&
                currentChannel->Value() == failedChannel)
            {
                ChannelPromise.Reset();
            }
        }
    }

    TNullable<TDuration> DefaultTimeout;
    TChannelProducer Producer;

    TSpinLock SpinLock;
    TPromise< TValueOrError<IChannel::TPtr> > ChannelPromise;

};

IChannel::TPtr CreateRoamingChannel(
    TNullable<TDuration> defaultTimeout,
    TChannelProducer producer)
{
    return New<TRoamingChannel>(
        defaultTimeout,
        producer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
