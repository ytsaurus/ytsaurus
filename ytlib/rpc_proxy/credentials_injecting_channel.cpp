#include "credentials_injecting_channel.h"

#include <yt/core/rpc/client.h>
#include <yt/core/rpc/channel_detail.h>

#include <yt/ytlib/rpc_proxy/proto/api_service.pb.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TTokenInjectingChannel
    : public TChannelWrapper
{
public:
    TTokenInjectingChannel(
        IChannelPtr underlyingChannel,
        const TString& user,
        const TString& token,
        const TString& userIP)
        : TChannelWrapper(std::move(underlyingChannel))
        , User_(user)
        , Token_(token)
        , UserIP_(userIP)
    { }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        request->SetUser(User_);
        auto* ext = request->Header().MutableExtension(NProto::TCredentialsExt::credentials_ext);
        ext->set_token(Token_);
        ext->set_user_ip(UserIP_);
        return UnderlyingChannel_->Send(
            std::move(request),
            std::move(responseHandler),
            options);
    }

private:
    const TString User_;
    const TString Token_;
    const TString UserIP_;
};

IChannelPtr CreateTokenInjectingChannel(
    IChannelPtr underlyingChannel,
    const TString& user,
    const TString& token,
    const TString& userIP)
{
    YCHECK(underlyingChannel);
    return New<TTokenInjectingChannel>(
        std::move(underlyingChannel),
        user,
        token,
        userIP);
}

////////////////////////////////////////////////////////////////////////////////

class TCookieInjectingChannel
    : public TChannelWrapper
{
public:
    TCookieInjectingChannel(
        IChannelPtr underlyingChannel,
        const TString& user,
        const TString& domain,
        const TString& sessionId,
        const TString& sslSessionId,
        const TString& userIP)
        : TChannelWrapper(std::move(underlyingChannel))
        , User_(user)
        , Domain_(domain)
        , SessionId_(sessionId)
        , SslSessionId_(sslSessionId)
        , UserIP_(userIP)
    { }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        request->SetUser(User_);
        auto* ext = request->Header().MutableExtension(NProto::TCredentialsExt::credentials_ext);
        ext->set_domain(Domain_);
        ext->set_session_id(SessionId_);
        ext->set_ssl_session_id(SslSessionId_);
        ext->set_user_ip(UserIP_);
        return UnderlyingChannel_->Send(
            std::move(request),
            std::move(responseHandler),
            options);
    }

private:
    const TString User_;
    const TString Domain_;
    const TString SessionId_;
    const TString SslSessionId_;
    const TString UserIP_;
};

IChannelPtr CreateCookieInjectingChannel(
    IChannelPtr underlyingChannel,
    const TString& user,
    const TString& domain,
    const TString& sessionId,
    const TString& sslSessionId,
    const TString& userIP)
{
    YCHECK(underlyingChannel);
    return New<TCookieInjectingChannel>(
        std::move(underlyingChannel),
        user,
        domain,
        sessionId,
        sslSessionId,
        userIP);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
