#ifndef API_SERVICE_IMPL_INL_H_
#error "Direct inclusion of this file is not allowed, include api_service_impl.h"
// For the sake of sane code completion.
#include "api_service_impl.h"
#endif

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponseMessage>
void TApiServiceContext<TRequestMessage, TResponseMessage>::Reply(const TError& error)
{
    if (Client_ && Client_->GetNativeConnection()->IsTerminated()) {
        auto replyError = TError(NRpc::EErrorCode::TransportError, "Connection to cluster %v was terminated", ClientClusterName_);
        if (!error.IsOK()) {
            replyError.Add(error);
        }
        TBase::Reply(replyError);
    } else {
        TBase::Reply(error);
    }
}

template <class TRequestMessage, class TResponseMessage>
void TApiServiceContext<TRequestMessage, TResponseMessage>::SetLogger(NLogging::TLogger logger)
{
    Logger = std::move(logger);
}

template <class TRequestMessage, class TResponseMessage>
void TApiServiceContext<TRequestMessage, TResponseMessage>::SetClient(std::optional<std::string> clientClusterName, NApi::NNative::IClientPtr client)
{
    ClientClusterName_ = std::move(clientClusterName);
    Client_ = std::move(client);
}

template <class TRequestMessage, class TResponseMessage>
void TApiServiceContext<TRequestMessage, TResponseMessage>::SetupMainMessage(NYson::TYsonString requestYson)
{
    EmitMain_ = true;
    RequestYson_ = std::move(requestYson);
}

template <class TRequestMessage, class TResponseMessage>
void TApiServiceContext<TRequestMessage, TResponseMessage>::SetupErrorMessage()
{
    EmitError_ = true;
}

template <class TRequestMessage, class TResponseMessage>
void TApiServiceContext<TRequestMessage, TResponseMessage>::LogStructuredError(const TError& /*error*/) const
{
    LogStructured();
}

template <class TRequestMessage, class TResponseMessage>
void TApiServiceContext<TRequestMessage, TResponseMessage>::LogStructured() const
{
    // Throwing an exception here leads to a double reply, so wrap with try-catch.
    try {
        if (EmitMain_) {
            DoEmitMain();
        }
        if (EmitError_) {
            DoEmitError();
        }
    } catch (const std::exception& ex) {
        YT_TLOG_ERROR("Error while logging structured event")
            .With(ex);
    }
}

template <class TRequestMessage, class TResponseMessage>
TError TApiServiceContext<TRequestMessage, TResponseMessage>::GetFinalError() const
{
    return this->IsCanceled() ? MakeCanceledError() : this->GetError();
}

template <class TRequestMessage, class TResponseMessage>
void TApiServiceContext<TRequestMessage, TResponseMessage>::DoEmitMain() const
{
    // This topic is a complete verbose structured logging, similar to HTTP proxy structured logging.
    // Note that message contains full request body encoded in YSON, so messages may be really heavy.
    // At production clusters, messages from this topic should not be emitted for the most frequently
    // invoked methods like LookupRows or ModifyRows.

    const auto& header = this->GetRequestHeader();
    const auto& credentialsExt = header.GetExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
    auto hashedCredentials = NAuth::HashCredentials(credentialsExt);
    const auto& traceContext = this->GetTraceContext();

    const auto& error = GetFinalError();
    auto errorCodeSet = error.GetDistinctNonTrivialErrorCodes();
    std::vector<TErrorCode> errorCodes(errorCodeSet.begin(), errorCodeSet.end());
    std::sort(errorCodes.begin(), errorCodes.end());

    NLogging::LogStructuredEventFluently(RpcProxyStructuredLoggerMain(), NLogging::ELogLevel::Info)
        .Item("request_id").Value(this->GetRequestId())
        .Item("endpoint").Value(this->GetEndpointAttributes())
        .Item("method").Value(this->GetMethod())
        .OptionalItem("path", RequestPath_)
        .Item("request").Value(RequestYson_)
        .Item("identity").Value(this->GetAuthenticationIdentity())
        .Item("credentials").Value(hashedCredentials)
        .Item("is_retry").Value(this->IsRetry())
        .OptionalItem("mutation_id", this->GetMutationId())
        .OptionalItem("realm_id", this->GetRealmId())
        // Zero-value corresponds to default tos level, so OptionalItem works reasonably,
        // producing item only if tos level is different from default one.
        .OptionalItem("tos_level", header.tos_level())
        .DoIf(static_cast<bool>(traceContext), [&] (auto fluent) {
            fluent
                .Item("trace_id").Value(traceContext->GetTraceId());
        })
        .Item("error").Value(error)
        .Item("error_skeleton").Value(error.GetSkeleton())
        .Item("error_codes").Value(errorCodes)
        .OptionalItem("user_agent", YT_OPTIONAL_FROM_PROTO(header, user_agent))
        .OptionalItem("request_body_size", NRpc::GetMessageBodySize(this->GetRequestMessage()))
        .OptionalItem("request_attachment_total_size", NRpc::GetTotalMessageAttachmentSize(this->GetRequestMessage()))
        .DoIf(!this->IsCanceled(), [&] (auto fluent) {
            fluent
                .OptionalItem("response_body_size", NRpc::GetMessageBodySize(this->GetResponseMessage()))
                .OptionalItem("response_attachment_total_size", NRpc::GetTotalMessageAttachmentSize(this->GetResponseMessage()));
        })
        .OptionalItem("client_start_time", this->GetStartTime())
        .OptionalItem("timeout", this->GetTimeout())
        .Item("arrive_instant").Value(this->GetArriveInstant())
        .Item("wait_time").Value(this->GetWaitDuration())
        .Item("execution_time").Value(this->GetExecutionDuration())
        .Item("finish_instant").Value(this->GetFinishInstant())
        .OptionalItem("cpu_time", this->GetTraceContextTime())
        .DoIf(credentialsExt.has_user_ticket() && !credentialsExt.has_service_ticket(), [&] (auto fluent) {
            fluent
                .Item("debug_info").Value(NYTree::BuildYsonStringFluently()
                    .BeginMap()
                        .Item("user_ticket_and_no_service_ticket").Value(true)
                    .EndMap());
        });
}

template <class TRequestMessage, class TResponseMessage>
void TApiServiceContext<TRequestMessage, TResponseMessage>::DoEmitError() const
{
    // This topic is designated for (primarily dyntable) error analytics reasons.
    // It is expected to be significantly lighter than main topic, so it is enabled
    // for all methods by default. Messages are emitted only for requests resulted
    // in errors.

    const auto& error = GetFinalError();
    if (error.IsOK()) {
        return;
    }

    auto errorCodeSet = error.GetDistinctNonTrivialErrorCodes();
    std::vector<TErrorCode> errorCodes(errorCodeSet.begin(), errorCodeSet.end());
    std::sort(errorCodes.begin(), errorCodes.end());

    NLogging::LogStructuredEventFluently(RpcProxyStructuredLoggerError(), NLogging::ELogLevel::Info)
        .Item("request_id").Value(this->GetRequestId())
        .Item("endpoint").Value(this->GetEndpointAttributes())
        .Item("method").Value(this->GetMethod())
        .OptionalItem("path", RequestPath_)
        .Item("identity").Value(this->GetAuthenticationIdentity())
        .Item("error").Value(error)
        .Item("error_skeleton").Value(error.GetSkeleton())
        .Item("error_codes").Value(errorCodes);
        // TODO(max42): YT-15042. Add error skeleton.
}

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
void SetMutatingOptions(
    NApi::TMutatingOptions* options,
    const TRequest* request,
    const NRpc::IServiceContext* context)
{
    if (request->has_mutating_options()) {
        FromProto(options, request->mutating_options());
    }
    const auto& header = context->RequestHeader();
    if (header.retry()) {
        options->Retry = true;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TResponse, class TRow>
std::vector<TSharedRef> PrepareRowsetForAttachment(
    TResponse* response,
    const TIntrusivePtr<NApi::IRowset<TRow>>& rowset,
    const IMemoryUsageTrackerPtr& memoryTracker)
{
    auto attachments = NApi::NRpcProxy::SerializeRowset(
        *rowset->GetSchema(),
        rowset->GetRows(),
        response->mutable_rowset_descriptor());

    if (memoryTracker) {
        for (auto& attachment : attachments) {
            attachment = memoryTracker->Track(attachment);
        }
    }

    return attachments;
}

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponseMessage>
void TMasterMetadataApiService::InitContext(TApiServiceContext<TRequestMessage, TResponseMessage>* context)
{
    using TContext = NYT::NRpcProxy::TApiServiceContext<TRequestMessage, TResponseMessage>;

    context->SetLogger(Logger
        .WithTag("RequestId", context->GetRequestId()));

    // First, recover request path from the typed request context using the incredible power of C++20 concepts.
    std::optional<NYPath::TYPath> requestPath;
    if constexpr (requires { context->Request().path(); }) {
        requestPath.emplace(context->Request().path());
    }
    context->SetRequestPath(std::move(requestPath));

    // Then, connect it to the typed context using subscriptions for reply and cancel signals.
    context->SubscribeReplied(BIND(&TContext::LogStructured, MakeWeak(context)));
    context->SubscribeCanceled(BIND(&TContext::LogStructuredError, MakeWeak(context)));

    // Finally, setup structured logging messages to be emitted.

    auto shouldEmit = [method = context->GetMethod()] (const TStructuredLoggingTopicDynamicConfigPtr& config) {
        return config->Enable &&
            !config->SuppressedMethods.contains(method) &&
            config->Methods.Value(method, DefaultMethodConfig)->Enable;
    };

    const auto& config = Config_.Acquire();

    // NB: We try to do heavy work only if we are actually going to omit corresponding message. Conserve priceless CPU time.
    if (shouldEmit(config->StructuredLoggingMainTopic)) {
        std::string requestYson;
        TStdStringOutput requestOutput(requestYson);
        NYson::TYsonWriter requestYsonWriter(&requestOutput, NYson::EYsonFormat::Text);
        NYson::TProtobufParserOptions parserOptions{
            .SkipUnknownFields = true,
            .Utf8Check = NYson::EUtf8Check::Disable,
        };

        const TRequestMessage* request = &context->Request();
        TRequestMessage copy;

        if constexpr (std::is_same_v<TRequestMessage, NApi::NRpcProxy::NProto::TReqSelectRows>) {
            if (std::ssize(request->query()) > config->StructuredLoggingQueryTruncationSize) {
                copy = *request;
                copy.mutable_query()->resize(config->StructuredLoggingQueryTruncationSize);
                request = &copy;
            }
        }

        const auto& methodConfig =  config->StructuredLoggingMainTopic->Methods.Value(context->GetMethod(), DefaultMethodConfig);
        const auto maxRequestSize = methodConfig->MaxRequestByteSize.value_or(config->StructuredLoggingMaxRequestByteSize);

        if (static_cast<i64>(request->ByteSizeLong()) <= maxRequestSize) {
            NYson::WriteProtobufMessage(&requestYsonWriter, *request, parserOptions);
        } else {
            requestYsonWriter.OnEntity();
        }

        context->SetupMainMessage(NYson::TYsonString(std::move(requestYson)));
    }

    if (shouldEmit(config->StructuredLoggingErrorTopic)) {
        context->SetupErrorMessage();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TContext, class TExecutor, class TResultHandler>
class TMasterMetadataApiService::TExecuteCallSession
    : public TRefCounted
{
public:
    TExecuteCallSession(
        TIntrusivePtr<TMasterMetadataApiService> apiService,
        TIntrusivePtr<TContext> context,
        TExecutor&& executor,
        TResultHandler&& resultHandler)
        : ApiService_(std::move(apiService))
        , Context_(std::move(context))
        , Executor_(std::move(executor))
        , ResultHandler_(std::move(resultHandler))
    { }

    void Run()
    {
        auto future = Executor_();

        using TResult = typename decltype(future)::TValueType;

        future.Subscribe(
            BIND([this, this_ = MakeStrong(this)] (const TErrorOr<TResult>& resultOrError) {
                if (!resultOrError.IsOK()) {
                    HandleError(resultOrError);
                    return;
                }

                try {
                    if constexpr(std::is_same_v<TResult, void>) {
                        ResultHandler_(Context_);
                    } else {
                        ResultHandler_(Context_, resultOrError.Value());
                    }
                    Context_->Reply();
                } catch (const std::exception& ex) {
                    HandleError(ex);
                } catch (const NConcurrency::TFiberCanceledException&) {
                    // Intentionally do nothing.
                }
            }));

        Context_->SubscribeCanceled(BIND([future = std::move(future)] (const TError& error) {
            future.Cancel(error);
        }));
    }

private:
    const TIntrusivePtr<TMasterMetadataApiService> ApiService_;
    const TIntrusivePtr<TContext> Context_;
    const TExecutor Executor_;
    const TResultHandler ResultHandler_;

    void HandleError(const TError& error)
    {
        auto wrappedError = TError(error.GetCode(), "Internal RPC call failed")
            .With(error);
        // If request contains path (e.g. GetNode), enrich error with it.
        if constexpr (requires { Context_->Request().path(); }) {
            wrappedError = wrappedError
                .With("path", Context_->Request().path());
        }
        Context_->Reply(std::move(wrappedError));
    }
};

template <class TContext, class TExecutor, class TResultHandler>
void TMasterMetadataApiService::ExecuteCall(
    TIntrusivePtr<TContext> context,
    TExecutor&& executor,
    TResultHandler&& resultHandler)
{
    New<TExecuteCallSession<TContext, TExecutor, TResultHandler>>(
        this,
        std::move(context),
        std::forward<TExecutor>(executor),
        std::forward<TResultHandler>(resultHandler))
        ->Run();
}

template <class TContext, class TExecutor>
void TMasterMetadataApiService::ExecuteCall(
    const TIntrusivePtr<TContext>& context,
    TExecutor&& executor)
{
    ExecuteCall(
        context,
        std::forward<TExecutor>(executor),
        [] (const TIntrusivePtr<TContext>& /*context*/) { });
}

////////////////////////////////////////////////////////////////////////////////

template <typename TContext, typename TRequest>
std::optional<NFormats::TFormat> TApiService::GetFormat(
    const TContext& context,
    const TRequest& request)
{
    std::optional<NFormats::TFormat> format;
    if (request->has_format()) {
        auto stringFormat = NYson::TYsonStringBuf(request->format());
        ValidateFormat(context->GetAuthenticationIdentity().User, NYTree::ConvertToNode(stringFormat));
        format = NYTree::ConvertTo<NFormats::TFormat>(stringFormat);
    }

    return format;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
