#include "channel.h"
#include "helpers.h"

#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/static_channel_factory.h>
#include <yt/yt/core/rpc/channel_detail.h>

#include <util/datetime/base.h>

namespace NYT::NHydraStressTest {

using namespace NConcurrency;
using namespace NElection;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HydraStressTestLogger;

////////////////////////////////////////////////////////////////////////////////

const TString& TPeerChannel::GetEndpointDescription() const
{
    return Underlying_->GetEndpointDescription();
}

const IAttributeDictionary& TPeerChannel::GetEndpointAttributes() const
{
    return Underlying_->GetEndpointAttributes();
}

IClientRequestControlPtr TPeerChannel::Send(
    IClientRequestPtr request,
    IClientResponseHandlerPtr responseHandler,
    const TSendOptions& options)
{
    auto error = TError(NRpc::EErrorCode::TransportError, "Channel is broken");

    if (Broken_) {
        auto errorKind = rand() % 3;
        if (errorKind == 0) {
            auto timeout = FromProto<TDuration>(request->Header().timeout());
            TDelayedExecutor::Submit(
                BIND(&IClientResponseHandler::HandleError, responseHandler, error),
                timeout);
            YT_LOG_DEBUG("Request timeout");
            return nullptr;
        }

        if (errorKind == 1) {
            responseHandler->HandleError(std::move(error));
            return nullptr;
        }
    }

    auto thunk = New<TClientRequestControlThunk>();
    TDelayedExecutor::Submit(
        BIND([=, this, this_ = MakeStrong(this)] {
            auto requestControl = Underlying_->Send(request, responseHandler, options);
            thunk->SetUnderlying(requestControl);
        }),
        TDuration::MilliSeconds(rand() % 100));

    if (Broken_) {
        return nullptr;
    }
    return thunk;
}

void TPeerChannel::Terminate(const TError& error)
{
    Underlying_->Terminate(error);
}

void TPeerChannel::SubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    Underlying_->SubscribeTerminated(callback);
}

void TPeerChannel::UnsubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    Underlying_->UnsubscribeTerminated(callback);
}

void TPeerChannel::SetUnderlying(IChannelPtr underlying)
{
    Underlying_ = underlying;
}

void TPeerChannel::SetBroken(bool broken)
{
    Broken_.store(broken);
}

int TPeerChannel::GetInflightRequestCount()
{
    return Underlying_->GetInflightRequestCount();
}

//////////////////////////////////////////////////////////////////////////////////

TChannelManager::TChannelManager(int peerCount)
{
    Channels_.resize(peerCount);

    for (int from = 0; from < peerCount; ++from) {
        Channels_[from].resize(peerCount);
        ChannelFactories_.push_back(New<TStaticChannelFactory>());
        for (int to = 0; to < peerCount; ++to) {
            auto channel = New<TPeerChannel>();
            Channels_[from][to] = channel;
            ChannelFactories_[from]->Add(GetPeerAddress(to), channel);
        }
    }
}

TStaticChannelFactoryPtr TChannelManager::GetChannelFactory(int peerId)
{
    return ChannelFactories_[peerId];
}

void TChannelManager::SetUnderlying(IChannelPtr channel, int peerId)
{
    for (auto& channels: Channels_) {
        channels[peerId]->SetUnderlying(channel);
    }
}

void TChannelManager::SetBroken(int from, int to, bool broken)
{
    Channels_[from][to]->SetBroken(broken);
    Channels_[to][from]->SetBroken(broken);
}

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
