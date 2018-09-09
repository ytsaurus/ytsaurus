#include "credentials_injecting_channel.h"

#include <yt/core/rpc/client.h>
#include <yt/core/rpc/channel_detail.h>
#include <yt/core/rpc/proto/rpc.pb.h>

namespace NYT {
namespace NApi {
namespace NRpcProxy {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TUserInjectingChannel
    : public TChannelWrapper
{
public:
    TUserInjectingChannel(IChannelPtr underlyingChannel, const TNullable<TString>& user)
        : TChannelWrapper(std::move(underlyingChannel))
        , User_(user)
    { }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        DoInject(request);

        return TChannelWrapper::Send(
            std::move(request),
            std::move(responseHandler),
            options);
    }

protected:
    virtual void DoInject(const IClientRequestPtr& request)
    {
        if (User_) {
            request->SetUser(*User_);
        }
        request->SetUserAgent("yt-cpp-rpc-client/1.0");
    }

private:
    const TNullable<TString> User_;
};

IChannelPtr CreateUserInjectingChannel(IChannelPtr underlyingChannel, const TNullable<TString>& user)
{
    YCHECK(underlyingChannel);
    return New<TUserInjectingChannel>(std::move(underlyingChannel), user);
}

////////////////////////////////////////////////////////////////////////////////

class TTokenInjectingChannel
    : public TUserInjectingChannel
{
public:
    TTokenInjectingChannel(
        IChannelPtr underlyingChannel,
        const TNullable<TString>& user,
        const TString& token)
        : TUserInjectingChannel(std::move(underlyingChannel), user)
        , Token_(token)
    { }

protected:
    virtual void DoInject(const IClientRequestPtr& request) override
    {
        TUserInjectingChannel::DoInject(request);

        auto* ext = request->Header().MutableExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        ext->set_token(Token_);
    }

private:
    const TString Token_;
};

IChannelPtr CreateTokenInjectingChannel(
    IChannelPtr underlyingChannel,
    const TNullable<TString>& user,
    const TString& token)
{
    YCHECK(underlyingChannel);
    return New<TTokenInjectingChannel>(
        std::move(underlyingChannel),
        user,
        token);
}

////////////////////////////////////////////////////////////////////////////////

class TCookieInjectingChannel
    : public TUserInjectingChannel
{
public:
    TCookieInjectingChannel(
        IChannelPtr underlyingChannel,
        const TNullable<TString>& user,
        const TString& sessionId,
        const TString& sslSessionId)
        : TUserInjectingChannel(std::move(underlyingChannel), user)
        , SessionId_(sessionId)
        , SslSessionId_(sslSessionId)
    { }

protected:
    virtual void DoInject(const IClientRequestPtr& request) override
    {
        TUserInjectingChannel::DoInject(request);

        auto* ext = request->Header().MutableExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        ext->set_session_id(SessionId_);
        ext->set_ssl_session_id(SslSessionId_);
    }

private:
    const TString SessionId_;
    const TString SslSessionId_;
};

IChannelPtr CreateCookieInjectingChannel(
    IChannelPtr underlyingChannel,
    const TNullable<TString>& user,
    const TString& sessionId,
    const TString& sslSessionId)
{
    YCHECK(underlyingChannel);
    return New<TCookieInjectingChannel>(
        std::move(underlyingChannel),
        user,
        sessionId,
        sslSessionId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NApi
} // namespace NYT
