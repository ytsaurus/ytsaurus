#include "credentials_injecting_channel.h"

#include <yt/core/rpc/client.h>
#include <yt/core/rpc/channel_detail.h>

#include <yt/ytlib/rpc_proxy/api_service.pb.h>

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
        const Stroka& user,
        const Stroka& token,
        const Stroka& userIP)
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
        ext->set_userip(UserIP_);
        return UnderlyingChannel_->Send(
            std::move(request),
            std::move(responseHandler),
            options);
    }

private:
    const Stroka User_;
    const Stroka Token_;
    const Stroka UserIP_;
};

IChannelPtr CreateTokenInjectingChannel(
    IChannelPtr underlyingChannel,
    const Stroka& user,
    const Stroka& token,
    const Stroka& userIP)
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
        const Stroka& user,
        const Stroka& domain,
        const Stroka& sessionId,
        const Stroka& sslSessionId,
        const Stroka& userIP)
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
        ext->set_sessionid(SessionId_);
        ext->set_sslsessionid(SslSessionId_);
        ext->set_userip(UserIP_);
        return UnderlyingChannel_->Send(
            std::move(request),
            std::move(responseHandler),
            options);
    }

private:
    const Stroka User_;
    const Stroka Domain_;
    const Stroka SessionId_;
    const Stroka SslSessionId_;
    const Stroka UserIP_;
};

IChannelPtr CreateCookieInjectingChannel(
    IChannelPtr underlyingChannel,
    const Stroka& user,
    const Stroka& domain,
    const Stroka& sessionId,
    const Stroka& sslSessionId,
    const Stroka& userIP)
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
