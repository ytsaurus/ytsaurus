#include "server.h"

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/private.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/server.h>

#include <yt/yt/core/rpc/message.h>
#include <yt/yt/core/rpc/service.h>
#include <yt/yt/core/rpc/server_detail.h>

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NRpc::NHttp {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NYTree;
using namespace NNet;
using namespace NYT::NHttp;
using namespace NYT::NBus;
using namespace NYT::NRpc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THttpReplyBus)
DECLARE_REFCOUNTED_CLASS(THttpHandler)

////////////////////////////////////////////////////////////////////////////////

TString ToHttpContentType(EMessageFormat format)
{
    switch (format) {
        case EMessageFormat::Protobuf:
            return "application/x-protobuf";
        case EMessageFormat::Json:
            return "application/json;charset=utf8";
        case EMessageFormat::Yson:
            return "application/x-yson";
        default:
            YT_ABORT();
    }
}

std::optional<EMessageFormat> FromHttpContentType(TStringBuf contentType)
{
    if (contentType == "application/x-protobuf") {
        return EMessageFormat::Protobuf;
    } else if (contentType.StartsWith("application/json")) {
        return EMessageFormat::Json;
    } else if (contentType == "application/x-yson") {
        return EMessageFormat::Yson;
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class THttpReplyBus
    : public IBus
{
public:
    THttpReplyBus(
        IResponseWriterPtr rsp,
        TString endpointAddress,
        IAttributeDictionaryPtr endpointAttributes,
        TNetworkAddress endpointNetworkAddress)
        : Rsp_(std::move(rsp))
        , EndpointAddress_(std::move(endpointAddress))
        , EndpointAttributes_(std::move(endpointAttributes))
        , EndpointNetworkAddress_(std::move(endpointNetworkAddress))
    { }

    const TString& GetEndpointDescription() const override
    {
        return EndpointAddress_;
    }

    const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    const TString& GetEndpointAddress() const override
    {
        return EndpointAddress_;
    }

    const NNet::TNetworkAddress& GetEndpointNetworkAddress() const override
    {
        return EndpointNetworkAddress_;
    }

    bool IsEndpointLocal() const override
    {
        return false;
    }

    bool IsEncrypted() const override
    {
        return false;
    }

    TBusNetworkStatistics GetNetworkStatistics() const override
    {
        return {};
    }

    TFuture<void> GetReadyFuture() const override
    {
        return VoidFuture;
    }

    TFuture<void> Send(TSharedRefArray message, const NBus::TSendOptions& /* options */) override
    {
        if (message.Size() > 2) {
            THROW_ERROR_EXCEPTION("Attachments are not supported in HTTP transport");
        }

        YT_VERIFY(message.Size() >= 1);
        NRpc::NProto::TResponseHeader responseHeader;
        YT_VERIFY(TryParseResponseHeader(message, &responseHeader));

        if (responseHeader.has_error() && responseHeader.error().code() != 0) {
            FillYTErrorHeaders(Rsp_, FromProto<TError>(responseHeader.error()));
            Rsp_->SetStatus(EStatusCode::BadRequest);
            auto replySent = Rsp_->Close();
            replySent.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TError& result){
                ReplySent_.Set(result);
            }));
            return replySent;
        }

        YT_VERIFY(message.Size() >= 2);
        if (responseHeader.has_format()) {
            auto format = CheckedEnumCast<EMessageFormat>(responseHeader.format());
            Rsp_->GetHeaders()->Add("Content-Type", ToHttpContentType(format));
        }

        FillYTErrorHeaders(Rsp_, TError{});
        Rsp_->SetStatus(EStatusCode::OK);
        auto bodySent = Rsp_->WriteBody(PopEnvelope(message[1]));
        bodySent.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TError& result){
            ReplySent_.Set(result);
        }));
        return bodySent;
    }

    void SetTosLevel(TTosLevel /* tosLevel */) override
    { }

    void Terminate(const TError& /* error */) override
    { }

    void SubscribeTerminated(const TCallback<void(const TError&)>& /* callback */) override
    {
        YT_UNIMPLEMENTED();
    }

    void UnsubscribeTerminated(const TCallback<void(const TError&)>& /* callback */) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> ReplySent()
    {
        return ReplySent_.ToFuture();
    }

private:
    const IResponseWriterPtr Rsp_;
    const TString EndpointAddress_;
    const IAttributeDictionaryPtr EndpointAttributes_;
    const TNetworkAddress EndpointNetworkAddress_;

    TPromise<void> ReplySent_ = NewPromise<void>();
};

DEFINE_REFCOUNTED_TYPE(THttpReplyBus)

////////////////////////////////////////////////////////////////////////////////

class THttpHandler
    : public IHttpHandler
{
public:
    THttpHandler(IServicePtr underlying, const TString& baseUrl, const NLogging::TLogger& logger)
        : Underlying_(std::move(underlying))
        , BaseUrl_(baseUrl)
        , Logger(logger)
    { }

    void HandleRequest(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp) override
    {
        if (MaybeHandleCors(req, rsp)) {
            return;
        }

        auto header = std::make_unique<NRpc::NProto::TRequestHeader>();
        auto error = TranslateRequest(req, header.get());
        if (!error.IsOK()) {
            FillYTErrorHeaders(rsp, error);
            rsp->SetStatus(EStatusCode::BadRequest);
            WaitFor(rsp->Close())
                .ThrowOnError();
            return;
        }

        auto body = req->ReadAll();
        auto endpointNetworkAddress = req->GetRemoteAddress();
        auto endpointAddress = ToString(endpointNetworkAddress);
        auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("address").Value(endpointAddress)
            .EndMap());
        auto replyBus = New<THttpReplyBus>(
            rsp,
            std::move(endpointAddress),
            std::move(endpointAttributes),
            std::move(endpointNetworkAddress));
        auto replySent = replyBus->ReplySent();

        auto requestMessage = CreateRequestMessage(*header, PushEnvelope(body), {});
        Underlying_->HandleRequest(std::move(header), std::move(requestMessage), std::move(replyBus));

        WaitFor(replySent)
            .ThrowOnError();
    }

private:
    const IServicePtr Underlying_;
    const TString BaseUrl_;
    const NLogging::TLogger Logger;


    TError TranslateRequest(const IRequestPtr& req, NRpc::NProto::TRequestHeader* rpcHeader)
    {
        if (req->GetMethod() != EMethod::Post) {
            return TError("Invalid method; POST expected");
        }

        const auto& url = req->GetUrl();
        if (url.Path.Size() <= BaseUrl_.Size()) {
            return TError("Invalid URL");
        }

        rpcHeader->set_service(Underlying_->GetServiceId().ServiceName);
        rpcHeader->set_method(TString(url.Path.SubStr(BaseUrl_.Size())));

        const auto& httpHeaders = req->GetHeaders();

        static const TString ContentTypeHeaderName("Content-Type");
        auto contentTypeString = httpHeaders->Find(ContentTypeHeaderName);
        if (contentTypeString) {
            auto decodedType = FromHttpContentType(*contentTypeString);
            if (!decodedType) {
                return TError("Invalid \"Content-Type\" header value")
                    << TErrorAttribute("value", *contentTypeString);
            }
            rpcHeader->set_request_format(static_cast<i32>(*decodedType));
        }

        static const TString RequestFormatOptionsHeaderName("X-YT-Request-Format-Options");
        auto requestFormatOptionsYson = httpHeaders->Find(RequestFormatOptionsHeaderName);
        if (requestFormatOptionsYson) {
            rpcHeader->set_request_format_options(*requestFormatOptionsYson);
        }

        static const TString AcceptHeaderName("Accept");
        auto acceptString = httpHeaders->Find(AcceptHeaderName);
        if (acceptString) {
            auto decodedType = FromHttpContentType(*acceptString);
            if (!decodedType) {
                return TError("Invalid \"Accept\" header value")
                    << TErrorAttribute("value", *acceptString);
            }
            rpcHeader->set_response_format(static_cast<i32>(*decodedType));
        }

        static const TString ResponseFormatOptionsHeaderName("X-YT-Response-Format-Options");
        auto responseFormatOptionsYson = httpHeaders->Find(ResponseFormatOptionsHeaderName);
        if (responseFormatOptionsYson) {
            rpcHeader->set_response_format_options(*responseFormatOptionsYson);
        }

        static const TString RequestIdHeaderName("X-YT-Request-Id");
        auto requestIdString = httpHeaders->Find(RequestIdHeaderName);
        TRequestId requestId;
        if (requestIdString) {
            if (!TRequestId::FromString(*requestIdString, &requestId)) {
                return TError("Invalid %Qv header value", RequestIdHeaderName)
                    << TErrorAttribute("value", *requestIdString);
            }
        } else {
            requestId = TRequestId::Create();
        }
        ToProto(rpcHeader->mutable_request_id(), requestId);

        auto getCredentialsExt = [&] {
            return rpcHeader->MutableExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        };

        static const TString AuthorizationHeaderName("Authorization");
        auto authorizationString = httpHeaders->Find(AuthorizationHeaderName);
        if (authorizationString) {
            const TStringBuf Prefix = "OAuth ";
            if (!authorizationString->StartsWith(Prefix)) {
                return TError("Invalid \"Authorization\" header value");
            }
            getCredentialsExt()->set_token(TrimLeadingWhitespaces(authorizationString->substr(Prefix.length())));
        }

        static const TString UserTicketHeaderName("X-Ya-User-Ticket");
        auto userTicketString = httpHeaders->Find(UserTicketHeaderName);
        if (userTicketString) {
            getCredentialsExt()->set_user_ticket(TrimLeadingWhitespaces(*userTicketString));
        }

        static const TString CookieHeaderName("Cookie");
        auto cookieString = httpHeaders->Find(CookieHeaderName);
        if (cookieString) {
            auto cookieMap = ParseCookies(*cookieString);

            static const TString SessionIdCookieName("Session_id");
            auto sessionIdIt = cookieMap.find(SessionIdCookieName);
            if (sessionIdIt != cookieMap.end()) {
                getCredentialsExt()->set_session_id(sessionIdIt->second);
            }

            static const TString SessionId2CookieName("sessionid2");
            auto sslSessionIdIt = cookieMap.find(SessionId2CookieName);
            if (sslSessionIdIt != cookieMap.end()) {
                getCredentialsExt()->set_ssl_session_id(sslSessionIdIt->second);
            }
        }

        static const TString UserAgentHeaderName("User-Agent");
        auto userAgent = httpHeaders->Find(UserAgentHeaderName);
        if (userAgent) {
            rpcHeader->set_user_agent(*userAgent);
        }

        static const TString UserTagHeaderName("X-YT-User-Tag");
        auto userTag = httpHeaders->Find(UserTagHeaderName);
        if (userTag) {
            rpcHeader->set_user_tag(*userTag);
        }

        return {};
    }
};

DEFINE_REFCOUNTED_TYPE(THttpHandler)

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public NRpc::TServerBase
{
public:
    explicit TServer(NYT::NHttp::IServerPtr httpServer)
        : TServerBase(HttpLogger.WithTag("ServerId: %v", TGuid::Create()))
        , HttpServer_(std::move(httpServer))
    { }

private:
    NYT::NHttp::IServerPtr HttpServer_;

    void DoStart() override
    {
        HttpServer_->Start();
        TServerBase::DoStart();
    }

    TFuture<void> DoStop(bool graceful) override
    {
        HttpServer_->Stop();
        HttpServer_.Reset();
        return TServerBase::DoStop(graceful);
    }

    void DoRegisterService(const IServicePtr& service) override
    {
        auto baseUrl = Format("/%v/", service->GetServiceId().ServiceName);
        HttpServer_->AddHandler(baseUrl, New<THttpHandler>(std::move(service), baseUrl, Logger));

        // COMPAT(babenko): remove this once fully migrated to new url scheme
        auto index = service->GetServiceId().ServiceName.find_last_of('.');
        if (index != TString::npos) {
            auto anotherBaseUrl = Format("/%v/", service->GetServiceId().ServiceName.substr(index + 1));
            HttpServer_->AddHandler(anotherBaseUrl, New<THttpHandler>(std::move(service), anotherBaseUrl, Logger));
        }
    }

    void DoUnregisterService(const IServicePtr& /*service*/) override
    {
        YT_ABORT();
    }
};

NRpc::IServerPtr CreateServer(NYT::NHttp::IServerPtr httpServer)
{
    return New<TServer>(std::move(httpServer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NHttp
