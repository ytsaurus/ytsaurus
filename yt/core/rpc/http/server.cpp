#include "server.h"

#include <yt/core/net/address.h>

#include <yt/core/http/http.h>
#include <yt/core/http/private.h>
#include <yt/core/http/helpers.h>
#include <yt/core/http/server.h>

#include <yt/core/rpc/message.h>
#include <yt/core/rpc/service.h>
#include <yt/core/rpc/server_detail.h>

#include <yt/core/bus/bus.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NRpc {
namespace NHttp {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NYTree;
using namespace NNet;
using namespace NYT::NHttp;
using namespace NYT::NBus;
using namespace NYT::NRpc;

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
            return "application/json";
        case EMessageFormat::Yson:
            return "application/x-yson";
        default:
            Y_UNREACHABLE();
    }
}

std::optional<EMessageFormat> FromHttpContentType(TStringBuf contentType)
{
    if (contentType == "application/x-protobuf") {
        return EMessageFormat::Protobuf;
    } else if (contentType == "application/json") {
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
    explicit THttpReplyBus(
        IResponseWriterPtr rsp,
        const TString& endpointDescription,
        std::unique_ptr<IAttributeDictionary> endpointAttributes,
        const TNetworkAddress& endpointAddress)
        : Rsp_(std::move(rsp))
        , EndpointDescription_(endpointDescription)
        , EndpointAttributes_(std::move(endpointAttributes))
        , EndpointAddress_(endpointAddress)
    { }

    virtual const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    virtual const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    virtual const NNet::TNetworkAddress& GetEndpointAddress() const override
    {
        return EndpointAddress_;
    }

    virtual TTcpDispatcherStatistics GetStatistics() const override
    {
        return {};
    }

    virtual TFuture<void> Send(TSharedRefArray message, const NBus::TSendOptions& /* options */) override
    {
        if (message.Size() > 2) {
            THROW_ERROR_EXCEPTION("Attachments are not supported in HTTP transport");
        }

        YCHECK(message.Size() >= 1);
        NRpc::NProto::TResponseHeader responseHeader;
        YCHECK(ParseResponseHeader(message, &responseHeader));

        if (responseHeader.has_error() && responseHeader.error().code() != 0) {
            FillYTErrorHeaders(Rsp_, FromProto<TError>(responseHeader.error()));
            Rsp_->SetStatus(EStatusCode::BadRequest);
            auto replySent = Rsp_->Close();
            replySent.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<void>& result){
                ReplySent_.Set(result);
            }));
            return replySent;
        }

        YCHECK(message.Size() >= 2);
        if (responseHeader.has_response_format()) {
            Rsp_->GetHeaders()->Add("Content-Type", ToHttpContentType(static_cast<EMessageFormat>(responseHeader.response_format())));
        }

        FillYTErrorHeaders(Rsp_, TError{});
        Rsp_->SetStatus(EStatusCode::OK);
        auto bodySent = Rsp_->WriteBody(PopEnvelope(message[1]));
        bodySent.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<void>& result){
            ReplySent_.Set(result);
        }));
        return bodySent;
    }

    virtual void SetTosLevel(TTosLevel /* tosLevel */) override
    { }

    virtual void Terminate(const TError& /* error */) override
    { }

    void SubscribeTerminated(const TCallback<void(const TError&)>& /* callback */)
    {
        Y_UNIMPLEMENTED();
    }

    void UnsubscribeTerminated(const TCallback<void(const TError&)>& /* callback */)
    {
        Y_UNIMPLEMENTED();
    }

    TFuture<void> ReplySent()
    {
        return ReplySent_.ToFuture();
    }

private:
    const IResponseWriterPtr Rsp_;
    const TString EndpointDescription_;
    const std::unique_ptr<IAttributeDictionary> EndpointAttributes_;
    const TNetworkAddress EndpointAddress_;

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

    virtual void HandleRequest(
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
        auto remoteAddress = req->GetRemoteAddress();
        auto endpointDescription = ToString(remoteAddress);
        auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("address").Value(endpointDescription)
            .EndMap());
        auto replyBus = New<THttpReplyBus>(
            rsp,
            endpointDescription,
            std::move(endpointAttributes),
            remoteAddress);
        auto replySent = replyBus->ReplySent();

        auto requestMessage = CreateRequestMessage(*header, PushEnvelope(body), {});
        Underlying_->HandleRequest(std::move(header), std::move(requestMessage), std::move(replyBus));

        WaitFor(replySent)
            .ThrowOnError();
    }

private:
    IServicePtr Underlying_;
    const TString BaseUrl_;
    const NLogging::TLogger Logger;


    TErrorOr<void> TranslateRequest(const IRequestPtr& req, NRpc::NProto::TRequestHeader* rpcHeader)
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

        static const TString RequestIdHeaderName("X-YT-Request-Id");
        auto requestIdString = httpHeaders->Find(RequestIdHeaderName);
        TRequestId requestId;
        if (requestIdString) {
            if (!TRequestId::FromString(*requestIdString, &requestId)) {
                return TError("Invalid \"X-YT-Request-Id\" header value")
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
            const auto Prefix = AsStringBuf("OAuth ");
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
        : TServerBase(NLogging::TLogger(HttpLogger)
            .AddTag("ServerId: %v", TGuid::Create()))
        , HttpServer_(std::move(httpServer))
    { }

private:
    NYT::NHttp::IServerPtr HttpServer_;

    virtual void DoStart() override
    {
        HttpServer_->Start();
        TServerBase::DoStart();
    }

    virtual TFuture<void> DoStop(bool graceful) override
    {
        HttpServer_->Stop();
        HttpServer_.Reset();
        return TServerBase::DoStop(graceful);
    }

    virtual void DoRegisterService(const IServicePtr& service) override
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

    virtual void DoUnregisterService(const IServicePtr& /*service*/) override
    {
        Y_UNREACHABLE();
    }
};

NRpc::IServerPtr CreateServer(NYT::NHttp::IServerPtr httpServer)
{
    return New<TServer>(std::move(httpServer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NRpc
} // namespace NYT
