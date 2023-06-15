#include "native_authenticating_channel.h"

#include "private.h"

#include <yt/yt/ytlib/auth/native_authentication_manager.h>

#include <yt/yt/library/tvm/tvm_base.h>

#include <yt/yt/library/auth_server/private.h>

#include <yt/yt/core/rpc/channel_detail.h>
#include <yt/yt/core/rpc/client.h>

namespace NYT::NAuth {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = NativeAuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TNativeAuthenticationInjectingChannel
    : public TChannelWrapper
{
public:
    TNativeAuthenticationInjectingChannel(
        IChannelPtr underlyingChannel,
        IServiceTicketAuthPtr ticketAuth)
        : TChannelWrapper(std::move(underlyingChannel))
        , TicketAuth_(std::move(ticketAuth))
        , NativeAuthenticationManager_(TNativeAuthenticationManager::Get())
    { }

    IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        try {
            DoInject(request);
        } catch (const std::exception& ex) {
            responseHandler->HandleError(TError(ex));
            return nullptr;
        }

        return TChannelWrapper::Send(
            std::move(request),
            std::move(responseHandler),
            options);
    }

private:
    const IServiceTicketAuthPtr TicketAuth_;
    TNativeAuthenticationManager* const NativeAuthenticationManager_;

    virtual void DoInject(const IClientRequestPtr& request)
    {
        if (!NativeAuthenticationManager_->IsSubmissionEnabled()) {
            return;
        }

        auto* ext = request->Header().MutableExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        ext->set_service_ticket(TicketAuth_->IssueServiceTicket());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNativeAuthenticationInjectingChannelFactory
    : public IChannelFactory
{
public:
    TNativeAuthenticationInjectingChannelFactory(
        IChannelFactoryPtr underlyingFactory,
        IServiceTicketAuthPtr ticketAuth)
        : UnderlyingFactory_(std::move(underlyingFactory))
        , TicketAuth_(std::move(ticketAuth))
    {
        YT_VERIFY(TicketAuth_);
    }

    IChannelPtr CreateChannel(const TString& address) override
    {
        auto channel = UnderlyingFactory_->CreateChannel(address);
        return New<TNativeAuthenticationInjectingChannel>(
            std::move(channel),
            TicketAuth_);
    }

private:
    const IChannelFactoryPtr UnderlyingFactory_;
    const IServiceTicketAuthPtr TicketAuth_;
};

////////////////////////////////////////////////////////////////////////////////

IChannelFactoryPtr CreateNativeAuthenticationInjectingChannelFactory(
    IChannelFactoryPtr channelFactory,
    std::optional<TTvmId> tvmId,
    IDynamicTvmServicePtr tvmService)
{
    if (!tvmId) {
        return channelFactory;
    }

    if (!tvmService) {
        tvmService = TNativeAuthenticationManager::Get()->GetTvmService();
    }

    if (!tvmService) {
        YT_LOG_ERROR("Cluster connection requires TVM authentification, but TVM service is unset");
        return channelFactory;
    }

    auto ticketAuth = CreateServiceTicketAuth(tvmService, *tvmId);
    return New<TNativeAuthenticationInjectingChannelFactory>(
        std::move(channelFactory),
        std::move(ticketAuth));
}

IChannelPtr CreateNativeAuthenticationInjectingChannel(
    IChannelPtr channel,
    std::optional<TTvmId> tvmId,
    IDynamicTvmServicePtr tvmService)
{
    if (!tvmId) {
        return channel;
    }

    if (!tvmService) {
        tvmService = TNativeAuthenticationManager::Get()->GetTvmService();
    }

    if (!tvmService) {
        YT_LOG_ERROR("Cluster connection requires TVM authentification, but TVM service is unset");
        return channel;
    }

    auto ticketAuth = CreateServiceTicketAuth(tvmService, *tvmId);
    return New<TNativeAuthenticationInjectingChannel>(
        std::move(channel),
        std::move(ticketAuth));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
