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
        const Stroka& userIp)
        : TChannelWrapper(std::move(underlyingChannel))
        , User_(user)
        , Token_(token)
        , UserIp_(userIp)
    { }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override
    {
        request->SetUser(User_);
        auto* ext = request->Header().MutableExtension(NProto::TCredentialsExt::credentials_ext);
        ext->set_token(Token_);
        ext->set_userip(UserIp_);
        return UnderlyingChannel_->Send(
            std::move(request),
            std::move(responseHandler),
            timeout,
            requestAck);
    }

private:
    const Stroka User_;
    const Stroka Token_;
    const Stroka UserIp_;
};

IChannelPtr CreateTokenInjectingChannel(
    IChannelPtr underlyingChannel,
    const Stroka& user,
    const Stroka& token,
    const Stroka& userIp)
{
    YCHECK(underlyingChannel);
    return New<TTokenInjectingChannel>(
        std::move(underlyingChannel),
        user,
        token,
        userIp);
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
        const Stroka& userIp)
        : TChannelWrapper(std::move(underlyingChannel))
        , User_(user)
        , Domain_(domain)
        , SessionId_(sessionId)
        , SslSessionId_(sslSessionId)
        , UserIp_(userIp)
    { }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override
    {
        request->SetUser(User_);
        auto* ext = request->Header().MutableExtension(NProto::TCredentialsExt::credentials_ext);
        ext->set_domain(Domain_);
        ext->set_sessionid(SessionId_);
        ext->set_sslsessionid(SslSessionId_);
        ext->set_userip(UserIp_);
        return UnderlyingChannel_->Send(
            std::move(request),
            std::move(responseHandler),
            timeout,
            requestAck);
    }

private:
    const Stroka User_;
    const Stroka Domain_;
    const Stroka SessionId_;
    const Stroka SslSessionId_;
    const Stroka UserIp_;
};

IChannelPtr CreateCookieInjectingChannel(
    IChannelPtr underlyingChannel,
    const Stroka& user,
    const Stroka& domain,
    const Stroka& sessionId,
    const Stroka& sslSessionId,
    const Stroka& userIp)
{
    YCHECK(underlyingChannel);
    return New<TCookieInjectingChannel>(
        std::move(underlyingChannel),
        user,
        domain,
        sessionId,
        sslSessionId,
        userIp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
