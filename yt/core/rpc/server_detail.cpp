#include "stdafx.h"
#include "server_detail.h"
#include "private.h"
#include "message.h"
#include "config.h"

namespace NYT {
namespace NRpc {

using namespace NConcurrency;
using namespace NBus;
using namespace NYTree;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcServerLogger;

////////////////////////////////////////////////////////////////////////////////

TServiceContextBase::TServiceContextBase(
    std::unique_ptr<TRequestHeader> header,
    TSharedRefArray requestMessage,
    const NLog::TLogger& logger,
    NLog::ELogLevel logLevel)
    : RequestHeader_(std::move(header))
    , RequestMessage_(std::move(requestMessage))
    , Logger(logger)
    , LogLevel_(logLevel)
{
    Initialize();
}

TServiceContextBase::TServiceContextBase(
    TSharedRefArray requestMessage,
    const NLog::TLogger& logger,
    NLog::ELogLevel logLevel)
    : RequestHeader_(new TRequestHeader())
    , RequestMessage_(std::move(requestMessage))
    , Logger(logger)
    , LogLevel_(logLevel)
{
    YCHECK(ParseRequestHeader(RequestMessage_, RequestHeader_.get()));
    Initialize();
}

void TServiceContextBase::Initialize()
{
    RequestId_ = RequestHeader_->has_request_id()
        ? FromProto<TRequestId>(RequestHeader_->request_id())
        : NullRequestId;

    RealmId_ = RequestHeader_->has_realm_id()
        ? FromProto<TRealmId>(RequestHeader_->realm_id())
        : NullRealmId;

    Replied_ = false;

    YASSERT(RequestMessage_.Size() >= 2);
    RequestBody_ = RequestMessage_[1];
    RequestAttachments_ = std::vector<TSharedRef>(
        RequestMessage_.Begin() + 2,
        RequestMessage_.End());
}

void TServiceContextBase::Reply(const TError& error)
{
    YASSERT(!Replied_);

    Error_ = error;
    Replied_ = true;

    if (IsOneWay()) {
        // Cannot reply OK to a one-way request.
        YCHECK(!error.IsOK());
    } else {
        DoReply();
    }

    if (AsyncResponseMessage_) {
        AsyncResponseMessage_.Set(GetResponseMessage());
    }

    if (Logger.IsEnabled(LogLevel_)) {
        LogResponse(error);
    }
}

void TServiceContextBase::Reply(TSharedRefArray responseMessage)
{
    YASSERT(!Replied_);
    YASSERT(!IsOneWay());
    YASSERT(responseMessage.Size() >= 1);

    // NB: One must parse responseMessage and only use its content since,
    // e.g., responseMessage may contain invalid request id.
    TResponseHeader header;
    YCHECK(ParseResponseHeader(responseMessage, &header));

    if (header.has_error()) {
        Error_ = FromProto<TError>(header.error());
    }
    if (Error_.IsOK()) {
        YASSERT(responseMessage.Size() >= 2);
        ResponseBody_ = responseMessage[1];
        ResponseAttachments_ = std::vector<TSharedRef>(
            responseMessage.Begin() + 2,
            responseMessage.End());
    } else {
        ResponseBody_.Reset();
        ResponseAttachments_.clear();
    }

    Replied_ = true;

    DoReply();

    if (AsyncResponseMessage_) {
        AsyncResponseMessage_.Set(GetResponseMessage());
    }
    
    LogResponse(Error_);
}

TFuture<TSharedRefArray> TServiceContextBase::GetAsyncResponseMessage() const
{
    YCHECK(!Replied_);

    if (!AsyncResponseMessage_) {
        AsyncResponseMessage_ = NewPromise<TSharedRefArray>();
    }

    return AsyncResponseMessage_;
}

TSharedRefArray TServiceContextBase::GetResponseMessage() const
{
    YCHECK(Replied_);

    if (!ResponseMessage_) {
        NProto::TResponseHeader header;
        ToProto(header.mutable_request_id(), RequestId_);
        ToProto(header.mutable_error(), Error_);

        ResponseMessage_ = Error_.IsOK()
            ? CreateResponseMessage(
                header,
                ResponseBody_,
                ResponseAttachments_)
            : CreateErrorResponseMessage(header);
    }

    return ResponseMessage_;
}

bool TServiceContextBase::IsOneWay() const
{
    return RequestHeader_->one_way();
}

bool TServiceContextBase::IsReplied() const
{
    return Replied_;
}

const TError& TServiceContextBase::GetError() const
{
    YASSERT(Replied_);

    return Error_;
}

TSharedRef TServiceContextBase::GetRequestBody() const
{
    return RequestBody_;
}

std::vector<TSharedRef>& TServiceContextBase::RequestAttachments()
{
    return RequestAttachments_;
}

TSharedRef TServiceContextBase::GetResponseBody()
{
    return ResponseBody_;
}

void TServiceContextBase::SetResponseBody(const TSharedRef& responseBody)
{
    YASSERT(!Replied_);
    YASSERT(!IsOneWay());

    ResponseBody_ = responseBody;
}

std::vector<TSharedRef>& TServiceContextBase::ResponseAttachments()
{
    YASSERT(!IsOneWay());

    return ResponseAttachments_;
}

TSharedRefArray TServiceContextBase::GetRequestMessage() const
{
    return RequestMessage_;
}

TRequestId TServiceContextBase::GetRequestId() const
{
    return RequestId_;
}

TNullable<TInstant> TServiceContextBase::GetRequestStartTime() const
{
    return
        RequestHeader_->has_request_start_time()
        ? TNullable<TInstant>(TInstant(RequestHeader_->request_start_time()))
        : Null;
}

TNullable<TInstant> TServiceContextBase::GetRetryStartTime() const
{
    return
        RequestHeader_->has_retry_start_time()
        ? TNullable<TInstant>(TInstant(RequestHeader_->retry_start_time()))
        : Null;
}

i64 TServiceContextBase::GetPriority() const
{
    return
        RequestHeader_->has_request_start_time()
        ? -RequestHeader_->request_start_time()
        : 0;
}

const Stroka& TServiceContextBase::GetService() const
{
    return RequestHeader_->service();
}

const Stroka& TServiceContextBase::GetMethod() const
{
    return RequestHeader_->method();
}

const TRealmId& TServiceContextBase::GetRealmId() const
{
    return RealmId_;
}

const TRequestHeader& TServiceContextBase::RequestHeader() const
{
    return *RequestHeader_;
}

TRequestHeader& TServiceContextBase::RequestHeader()
{
    return *RequestHeader_;
}

void TServiceContextBase::SetRawRequestInfo(const Stroka& info)
{
    RequestInfo_ = info;
    if (Logger.IsEnabled(LogLevel_)) {
        LogRequest();
    }
}

void TServiceContextBase::SetRawResponseInfo(const Stroka& info)
{
    YASSERT(!Replied_);
    YASSERT(!IsOneWay());

    ResponseInfo_ = info;
}

NLog::TLogger& TServiceContextBase::GetLogger()
{
    return Logger;
}

////////////////////////////////////////////////////////////////////////////////

TServiceContextWrapper::TServiceContextWrapper(IServiceContextPtr underlyingContext)
    : UnderlyingContext_(std::move(underlyingContext))
{ }

TSharedRefArray TServiceContextWrapper::GetRequestMessage() const
{
    return UnderlyingContext_->GetRequestMessage();
}

TRequestId TServiceContextWrapper::GetRequestId() const
{
    return UnderlyingContext_->GetRequestId();
}

TNullable<TInstant> TServiceContextWrapper::GetRequestStartTime() const
{
    return UnderlyingContext_->GetRequestStartTime();
}

TNullable<TInstant> TServiceContextWrapper::GetRetryStartTime() const
{
    return UnderlyingContext_->GetRetryStartTime();
}

i64 TServiceContextWrapper::GetPriority() const
{
    return UnderlyingContext_->GetPriority();
}

const Stroka& TServiceContextWrapper::GetService() const
{
    return UnderlyingContext_->GetService();
}

const Stroka& TServiceContextWrapper::GetMethod() const
{
    return UnderlyingContext_->GetMethod();
}

const TRealmId& TServiceContextWrapper::GetRealmId() const 
{
    return UnderlyingContext_->GetRealmId();
}

bool TServiceContextWrapper::IsOneWay() const
{
    return UnderlyingContext_->IsOneWay();
}

bool TServiceContextWrapper::IsReplied() const
{
    return UnderlyingContext_->IsReplied();
}

void TServiceContextWrapper::Reply(const TError& error)
{
    UnderlyingContext_->Reply(error);
}

void TServiceContextWrapper::Reply(TSharedRefArray responseMessage)
{
    UnderlyingContext_->Reply(responseMessage);
}

TFuture<TSharedRefArray> TServiceContextWrapper::GetAsyncResponseMessage() const
{
    return UnderlyingContext_->GetAsyncResponseMessage();
}

TSharedRefArray TServiceContextWrapper::GetResponseMessage() const
{
    return UnderlyingContext_->GetResponseMessage();
}

const TError& TServiceContextWrapper::GetError() const
{
    return UnderlyingContext_->GetError();
}

TSharedRef TServiceContextWrapper::GetRequestBody() const
{
    return UnderlyingContext_->GetRequestBody();
}

TSharedRef TServiceContextWrapper::GetResponseBody()
{
    return UnderlyingContext_->GetResponseBody();
}

void TServiceContextWrapper::SetResponseBody(const TSharedRef& responseBody)
{
    UnderlyingContext_->SetResponseBody(responseBody);
}

std::vector<TSharedRef>& TServiceContextWrapper::RequestAttachments()
{
    return UnderlyingContext_->RequestAttachments();
}

std::vector<TSharedRef>& TServiceContextWrapper::ResponseAttachments()
{
    return UnderlyingContext_->ResponseAttachments();
}

const NProto::TRequestHeader& TServiceContextWrapper::RequestHeader() const 
{
    return UnderlyingContext_->RequestHeader();
}

NProto::TRequestHeader& TServiceContextWrapper::RequestHeader()
{
    return UnderlyingContext_->RequestHeader();
}

void TServiceContextWrapper::SetRawRequestInfo(const Stroka& info)
{
    UnderlyingContext_->SetRawRequestInfo(info);
}

void TServiceContextWrapper::SetRawResponseInfo(const Stroka& info)
{
    UnderlyingContext_->SetRawResponseInfo(info);
}

NLog::TLogger& TServiceContextWrapper::GetLogger()
{
    return UnderlyingContext_->GetLogger();
}

////////////////////////////////////////////////////////////////////////////////

TServerBase::TServerBase()
{
    Started_ = false;
}

void TServerBase::RegisterService(IServicePtr service)
{
    YCHECK(service);

    auto serviceId = service->GetServiceId();

    {
        TWriterGuard guard(ServicesLock_);
        YCHECK(ServiceMap_.insert(std::make_pair(serviceId, service)).second);
        if (Config_) {
            auto it = Config_->Services.find(serviceId.ServiceName);
            if (it != Config_->Services.end()) {
                service->Configure(it->second);
            }
        }
    }

    LOG_INFO("RPC service registered (ServiceName: %v, RealmId: %v)",
        serviceId.ServiceName,
        serviceId.RealmId);
}

void TServerBase::UnregisterService(IServicePtr service)
{
    YCHECK(service);

    auto serviceId = service->GetServiceId();

    {
        TWriterGuard guard(ServicesLock_);
        YCHECK(ServiceMap_.erase(serviceId) == 1);
    }

    LOG_INFO("RPC service unregistered (ServiceName: %v, RealmId: %v)",
        serviceId.ServiceName,
        serviceId.RealmId);
}

IServicePtr TServerBase::FindService(const TServiceId& serviceId)
{
    TReaderGuard guard(ServicesLock_);
    auto it = ServiceMap_.find(serviceId);
    return it == ServiceMap_.end() ? nullptr : it->second;
}

void TServerBase::Configure(TServerConfigPtr config)
{
    TWriterGuard guard(ServicesLock_);

    // Future services will be configured appropriately.
    Config_ = config;

    // Apply configuration to all existing services.
    for (const auto& pair : config->Services) {
        const auto& serviceName = pair.first;
        const auto& serviceConfig = pair.second;
        auto services = DoFindServices(serviceName);
        for (auto service : services) {
            service->Configure(serviceConfig);
        }
    }
}

void TServerBase::Start()
{
    YCHECK(!Started_);

    DoStart();

    LOG_INFO("RPC server started");
}

void TServerBase::Stop()
{
    if (!Started_)
        return;

    DoStop();

    LOG_INFO("RPC server stopped");
}

void TServerBase::DoStart()
{
    Started_ = true;
}

void TServerBase::DoStop()
{
    Started_ = false;
}

std::vector<IServicePtr> TServerBase::DoFindServices(const Stroka& serviceName)
{
    std::vector<IServicePtr> result;
    for (const auto& pair : ServiceMap_) {
        if (pair.first.ServiceName == serviceName) {
            result.push_back(pair.second);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
