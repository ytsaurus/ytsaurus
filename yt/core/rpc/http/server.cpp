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

#include <yt/core/json/json_rpc_message_format.h>

namespace NYT {
namespace NRpc {
namespace NHttp {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT::NConcurrency;
using namespace NYT::NHttp;
using namespace NYT::NYTree;
using namespace NYT::NBus;
using namespace NYT::NRpc;
using namespace NYT::NJson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HttpLogger;

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

TNullable<EMessageFormat> FromHttpContentType(TStringBuf contentType)
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
        std::unique_ptr<IAttributeDictionary> endpointAttributes)
        : Rsp_(std::move(rsp))
        , EndpointDescription_(endpointDescription)
        , EndpointAttributes_(std::move(endpointAttributes))
    { }

    virtual const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    virtual const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
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
            Rsp_->WriteHeaders(EStatusCode::BadRequest);
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
        Rsp_->WriteHeaders(EStatusCode::Ok);
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

    TPromise<void> ReplySent_ = NewPromise<void>();
};

DEFINE_REFCOUNTED_TYPE(THttpReplyBus)

////////////////////////////////////////////////////////////////////////////////

class THttpHandler
    : public IHttpHandler
{
public:
    THttpHandler(IServicePtr underlying, const TString& baseUrl)
        : Underlying_(std::move(underlying))
        , BaseUrl_(baseUrl)
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
            rsp->WriteHeaders(EStatusCode::BadRequest);
            WaitFor(rsp->Close())
                .ThrowOnError();
            return;
        }

        auto body = req->ReadBody();
        auto endpointDescription = ToString(req->GetRemoteAddress());
        auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("address").Value(endpointDescription)
            .EndMap());
        auto replyBus = New<THttpReplyBus>(
            rsp,
            endpointDescription,
            std::move(endpointAttributes));
        auto replySent = replyBus->ReplySent();

        auto requestMessage = CreateRequestMessage(*header, PushEnvelope(body), {});
        Underlying_->HandleRequest(std::move(header), std::move(requestMessage), std::move(replyBus));

        WaitFor(replySent)
            .ThrowOnError();
    }

private:
    IServicePtr Underlying_;
    const TString BaseUrl_;

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

        auto contentType = req->GetHeaders()->Find("Content-Type");
        if (contentType) {
            auto decodedType = FromHttpContentType(*contentType);
            if (!decodedType) {
                return TError("Invalid header value")
                    << TErrorAttribute("header", "Content-Type")
                    << TErrorAttribute("value", *contentType);
            }

            rpcHeader->set_request_format(static_cast<i32>(*decodedType));
        }

        auto accept = req->GetHeaders()->Find("Accept");
        if (accept) {
            auto decodedType = FromHttpContentType(*accept);
            if (!decodedType) {
                return TError("Invalid header value")
                    << TErrorAttribute("header", "Accept")
                    << TErrorAttribute("value", *accept);
            }

            rpcHeader->set_response_format(static_cast<i32>(*decodedType));
        }

        auto requestIdString = req->GetHeaders()->Find("X-YT-Request-Id");
        TRequestId requestId;
        if (requestIdString) {
            if (!TRequestId::FromString(*requestIdString, &requestId)) {
                LOG_WARNING("Malformed request id, using a random one (MalformedRequestId: %v, RequestId: %v)",
                    *requestIdString,
                    requestId);
            }
        } else {
            requestId = TRequestId::Create();
        }
        ToProto(rpcHeader->mutable_request_id(), requestId);

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
        : TServerBase(HttpLogger)
        , HttpServer_(std::move(httpServer))
    {
        // XXX(babenko): try to get rid of this
        NJson::RegisterJsonRpcMessageFormat();
    }

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
        HttpServer_->AddHandler(baseUrl, New<THttpHandler>(std::move(service), baseUrl));

        // XXX(babenko): remove this once fully migrated to new url scheme
        auto index = service->GetServiceId().ServiceName.find_last_of('.');
        if (index != TString::npos) {
            auto anotherBaseUrl = Format("/%v/", service->GetServiceId().ServiceName.substr(index + 1));
            HttpServer_->AddHandler(anotherBaseUrl, New<THttpHandler>(std::move(service), anotherBaseUrl));
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
