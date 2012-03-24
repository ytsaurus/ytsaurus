#include "stdafx.h"
#include "leader_channel.h"

namespace NYT {
namespace NElection {

using namespace NBus;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TResponseHandlerWrapper
    : public IClientResponseHandler
{
public:
    typedef TIntrusivePtr<TResponseHandlerWrapper> TPtr;

    TResponseHandlerWrapper(
        IClientResponseHandler::TPtr underlyingHandler,
        IAction::TPtr onFailed)
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
            OnFailed->Do();
        }
    }

private:
    IClientResponseHandler::TPtr UnderlyingHandler;
    IAction::TPtr OnFailed;

};

////////////////////////////////////////////////////////////////////////////////

class TLeaderChannel
    : public IChannel
{
public:
    typedef TIntrusivePtr<TLeaderChannel> TPtr;

    TLeaderChannel(TLeaderLookup::TConfig::TPtr config)
        : Config(config)
        , LeaderLookup(New<TLeaderLookup>(~config))
    { }

    virtual TNullable<TDuration> GetDefaultTimeout() const
    {
        return Config->RpcTimeout;
    }

    virtual void Send(
        IClientRequest* request,
        IClientResponseHandler* responseHandler,
        TNullable<TDuration> timeout)
    {
        YASSERT(request);
        YASSERT(responseHandler);

        GetChannel()->Subscribe(FromMethod(
            &TLeaderChannel::OnGotChannel,
            MakeStrong(this),
            request,
            responseHandler,
            timeout));
    }

    virtual void Terminate()
    {
        TGuard<TSpinLock> guard(SpinLock);

        // TODO(babenko): this does not look correct
        // but we should get rid of Terminate soon anyway.
    
        IChannel::TPtr channel;
        if (PromissedChannel->TryGet(&channel)) {
            channel->Terminate();
        }
        PromissedChannel.Reset();
    }

private:
    friend class TResponseHandlerWrapper;

    TFuture<IChannel::TPtr>::TPtr GetChannel()
    {
        TGuard<TSpinLock> guard(SpinLock);
        
        if (PromissedChannel) {
            return PromissedChannel;
        }

        auto promisedChannel = PromissedChannel = New< TFuture<IChannel::TPtr> >();
        guard.Release();

        LeaderLookup->GetLeader()->Subscribe(FromMethod(
            &TLeaderChannel::OnLeaderFound,
            MakeStrong(this),
            promisedChannel));
        return promisedChannel;
    }

    void OnLeaderFound(TLeaderLookup::TResult result, TFuture<IChannel::TPtr>::TPtr promisedChannel)
    {
        if (result.Id == NElection::InvalidPeerId) {
            TGuard<TSpinLock> guard(SpinLock);
            if (PromissedChannel == promisedChannel) {
                promisedChannel->Set(NULL);
            }
        } else {
            auto channel = CreateBusChannel(result.Address);
            TGuard<TSpinLock> guard(SpinLock);
            if (PromissedChannel == promisedChannel) {
                promisedChannel->Set(channel);
            }
        }
    }
         
    void OnGotChannel(
        IChannel::TPtr channel,
        IClientRequest::TPtr request,
        IClientResponseHandler::TPtr responseHandler,
        TNullable<TDuration> timeout)
    {
        if (!channel) {
            responseHandler->OnError(TError(
                EErrorCode::Unavailable,
                "Unable to determine the leader"));
        } else {
            auto responseHandlerWrapper = New<TResponseHandlerWrapper>(
                ~responseHandler,
                FromMethod(&TLeaderChannel::OnChannelFailed, MakeStrong(this), channel));
            channel->Send(~request, ~responseHandlerWrapper, timeout);
        }
    }

    void OnChannelFailed(IChannel::TPtr failedChannel)
    {
        TGuard<TSpinLock> guard(SpinLock);
        IChannel::TPtr currentChannel;
        if (PromissedChannel->TryGet(&currentChannel) && currentChannel == failedChannel) {
            PromissedChannel.Reset();
        }
    }


    TLeaderLookup::TConfig::TPtr Config;
    TLeaderLookup::TPtr LeaderLookup;

    TSpinLock SpinLock;
    TFuture<IChannel::TPtr>::TPtr PromissedChannel;

};

IChannel::TPtr CreateLeaderChannel(TLeaderLookup::TConfig::TPtr config)
{
    return New<TLeaderChannel>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
