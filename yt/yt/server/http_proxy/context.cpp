#include "api.h"
#include "config.h"
#include "context.h"
#include "coordinator.h"
#include "formats.h"
#include "framing.h"
#include "helpers.h"
#include "http_authenticator.h"
#include "private.h"

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/rpc_proxy/public.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/poller.h>

#include <yt/yt/core/http/compression.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/http.h>

#include <yt/yt/core/json/config.h>
#include <yt/yt/core/json/json_writer.h>

#include <yt/yt/core/logging/fluent_log.h>

#include <yt/yt/core/rpc/authenticator.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/random/random.h>

#include <util/string/ascii.h>
#include <util/string/strip.h>

namespace NYT::NHttpProxy {

using namespace NConcurrency;
using namespace NDriver;
using namespace NFormats;
using namespace NHttp;
using namespace NLogging;
using namespace NObjectClient;
using namespace NScheduler;
using namespace NTracing;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TContext::TContext(
    TApiPtr api,
    IRequestPtr request,
    IResponseWriterPtr response)
    : Api_(std::move(api))
    , Request_(std::move(request))
    , Response_(std::move(response))
    , Logger(HttpProxyLogger.WithTag("RequestId: %v", Request_->GetRequestId()))
{
    DriverRequest_.Id = RandomNumber<ui64>();
}

bool TContext::TryPrepare()
{
    ProcessDebugHeaders(Request_, Response_, Api_->GetCoordinator());

    if (auto correlationId = Request_->GetHeaders()->Find("X-YT-Correlation-ID")) {
        Logger.AddTag("CorrelationId: %v", *correlationId);
    }

    Response_->GetHeaders()->Set("Cache-Control", "no-store");

    return
        TryParseRequest() &&
        TryParseCommandName() &&
        TryParseUser() &&
        TryInitFormatManager() &&
        TryGetDescriptor() &&
        TryCheckAvailability() &&
        TryRedirectHeavyRequests() &&
        TryGetHeaderFormat() &&
        TryGetInputFormat() &&
        TryGetInputCompression() &&
        TryGetOutputFormat() &&
        TryGetOutputCompression() &&
        TryAcquireConcurrencySemaphore();
}

bool TContext::TryParseRequest()
{
    auto userAgent = Request_->GetHeaders()->Find("User-Agent");
    if (userAgent && userAgent->find("Trident") != TString::npos) {
        // XXX(sandello): IE is bugged; it fails to parse request with trailing
        // headers that include colons. Remarkable.
        OmitTrailers_ = true;
    }

    if (Request_->GetHeaders()->Find("X-YT-Omit-Trailers")) {
        OmitTrailers_ = true;
    }

    if (Request_->GetHeaders()->Find("X-YT-Accept-Framing") && GetFramingConfig()->Enable) {
        Response_->GetHeaders()->Set("X-YT-Framing", "1");
        IsFramingEnabled_ = true;
    }

    return true;
}

bool TContext::TryParseCommandName()
{
    auto versionedName = TString(Request_->GetUrl().Path);
    versionedName.to_lower();

    if (versionedName == "/api" || versionedName == "/api/") {
        Response_->SetStatus(EStatusCode::OK);
        DispatchJson([&] (auto consumer) {
            BuildYsonFluently(consumer)
                .BeginList()
                    .Item().Value("v3")
                    .Item().Value("v4")
                .EndList();
        });
        return false;
    }

    if (versionedName.StartsWith("/api/v3")) {
        ApiVersion_ = 3;
    } else if (versionedName.StartsWith("/api/v4")) {
        ApiVersion_ = 4;
    } else {
        THROW_ERROR_EXCEPTION("Unsupported API version %Qv", versionedName);
    }

    TStringBuf commandName = versionedName;
    commandName.Skip(7);
    if (commandName == "" || commandName == "/") {
        if (*ApiVersion_ == 3) {
            Response_->SetStatus(EStatusCode::OK);
            DispatchJson([this] (auto consumer) {
                BuildYsonFluently(consumer)
                    .Value(Api_->GetDriverV3()->GetCommandDescriptors());
            });
        } else if (*ApiVersion_ == 4) {
            Response_->SetStatus(EStatusCode::OK);
            DispatchJson([this] (auto consumer) {
                BuildYsonFluently(consumer)
                    .Value(Api_->GetDriverV4()->GetCommandDescriptors());
            });
        }

        return false;
    }

    if (commandName[0] != '/') {
        DispatchNotFound("Malformed command name");
        return false;
    }

    commandName.Skip(1);
    for (auto c : commandName) {
        if (c != '_' && !IsAsciiAlpha(c)) {
            DispatchNotFound("Malformed command name");
            return false;
        }
    }
    DriverRequest_.CommandName = commandName;

    return true;
}

bool TContext::TryParseUser()
{
    // NB: This function is the only thing protecting cluster from
    // unauthorized requests. Please write code without bugs.

    auto authResult = Api_->GetHttpAuthenticator()->Authenticate(Request_);
    if (!authResult.IsOK()) {
        YT_LOG_DEBUG(authResult, "Authentication error");

        if (DriverRequest_.CommandName == "discover_proxies") {
            DriverRequest_.AuthenticatedUser = NSecurityClient::RootUserName;
            return true;
        }

        SetStatusFromAuthError(Response_, TError(authResult));
        FillYTErrorHeaders(Response_, TError(authResult));
        DispatchJson([&] (auto consumer) {
            BuildYsonFluently(consumer)
                .Value(TError(authResult));
        });
        return false;
    }

    Auth_ = authResult.Value();
    const auto& authenticatedUser = Auth_->Result.Login;

    if (auto setCookie = Auth_->Result.SetCookie) {
        Response_->GetHeaders()->Add("Set-Cookie", *setCookie);
    }

    if (DriverRequest_.CommandName == "ping_tx" || DriverRequest_.CommandName == "parse_ypath") {
        DriverRequest_.AuthenticatedUser = authenticatedUser;
        return true;
    }

    if (Api_->IsUserBannedInCache(authenticatedUser)) {
        Response_->SetStatus(EStatusCode::Forbidden);
        ReplyError(TError{Format("User %Qv is banned", authenticatedUser)});
        return false;
    }

    if (auto error = Api_->CheckAccess(authenticatedUser); !error.IsOK()) {
        YT_LOG_DEBUG(error);
        Response_->SetStatus(EStatusCode::Forbidden);
        auto proxyRole = Api_->GetCoordinator()->GetSelf()->Role;
        ReplyError(TError{Format("User %Qv is not allowed to access proxy with role %Qv", authenticatedUser, proxyRole)});
        return false;
    }

    DriverRequest_.AuthenticatedUser = authenticatedUser;
    DriverRequest_.UserRemoteAddress = Request_->GetRemoteAddress();
    return true;
}

bool TContext::TryInitFormatManager()
{
    FormatManager_ = std::make_unique<TFormatManager>(
        Api_->GetDynamicConfig()->Formats,
        DriverRequest_.AuthenticatedUser);
    return true;
}

bool TContext::TryGetDescriptor()
{
    if (*ApiVersion_ == 3) {
        Descriptor_ = Api_->GetDriverV3()->FindCommandDescriptor(DriverRequest_.CommandName);
    } else {
        Descriptor_ = Api_->GetDriverV4()->FindCommandDescriptor(DriverRequest_.CommandName);
    }

    if (!Descriptor_) {
        DispatchNotFound(Format("Command %Qv is not registered", DriverRequest_.CommandName));
        return false;
    }

    return true;
}

bool TContext::TryCheckAvailability()
{
    if (Api_->GetCoordinator()->IsBanned()) {
        DispatchUnavailable(TError{NApi::NRpcProxy::EErrorCode::ProxyBanned, "This proxy is banned"});
        return false;
    }

    return true;
}

bool TContext::TryRedirectHeavyRequests()
{
    bool suppressRedirect = Request_->GetHeaders()->Find("X-YT-Suppress-Redirect");
    if (Descriptor_->Heavy &&
        !Api_->GetCoordinator()->CanHandleHeavyRequests() &&
        !suppressRedirect &&
        !IsBrowserRequest(Request_))
    {
        if (Descriptor_->InputType != NFormats::EDataType::Null) {
            DispatchUnavailable(TError{"Control proxy may not serve heavy requests with input data"});
            return false;
        }

        RedirectToDataProxy(Request_, Response_, Api_->GetCoordinator());
        return false;
    }

    return true;
}

bool TContext::TryGetHeaderFormat()
{
    auto headerFormat = Request_->GetHeaders()->Find("X-YT-Header-Format");
    HeadersFormat_ = InferHeaderFormat(*FormatManager_, headerFormat);
    return true;
}

bool TContext::TryGetInputFormat()
{
    static const TString YtHeaderName = "X-YT-Input-Format";
    std::optional<TString> ytHeader;
    try {
        ytHeader = GatherHeader(Request_->GetHeaders(), YtHeaderName);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Unable to parse %v header", YtHeaderName)
            << ex;
    }
    auto contentTypeHeader = Request_->GetHeaders()->Find("Content-Type");
    InputFormat_ = InferFormat(
        *FormatManager_,
        YtHeaderName,
        *HeadersFormat_,
        ytHeader,
        "Content-Type",
        contentTypeHeader,
        /*isOutput*/ false,
        Descriptor_->InputType);
    return true;
}

bool TContext::TryGetInputCompression()
{
    auto header = Request_->GetHeaders()->Find("Content-Encoding");
    if (header) {
        auto compression = StripString(*header);
        if (!IsCompressionSupported(compression)) {
            Response_->SetStatus(EStatusCode::UnsupportedMediaType);
            ReplyError(TError{"Unsupported Content-Encoding"});
            return false;
        }

        InputContentEncoding_ = compression;
    } else {
        InputContentEncoding_ = IdentityContentEncoding;
    }

    return true;
}

bool TContext::TryGetOutputFormat()
{
    static const TString YtHeaderName = "X-YT-Output-Format";
    std::optional<TString> ytHeader;
    try {
        ytHeader = GatherHeader(Request_->GetHeaders(), YtHeaderName);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Unable to parse %v header", YtHeaderName)
            << ex;
    }
    auto acceptHeader = Request_->GetHeaders()->Find("Accept");
    OutputFormat_ = InferFormat(
        *FormatManager_,
        YtHeaderName,
        *HeadersFormat_,
        ytHeader,
        "Accept",
        acceptHeader,
        /*isOutput*/ true,
        Descriptor_->OutputType);
    return true;
}

bool TContext::TryGetOutputCompression()
{
    auto acceptEncodingHeader = Request_->GetHeaders()->Find("Accept-Encoding");
    if (acceptEncodingHeader) {
        auto contentEncoding = GetBestAcceptedEncoding(*acceptEncodingHeader);

        if (!contentEncoding.IsOK()) {
            Response_->SetStatus(EStatusCode::UnsupportedMediaType);
            ReplyError(contentEncoding);
            return false;
        }

        OutputContentEncoding_ = contentEncoding.Value();
    } else {
        OutputContentEncoding_ = IdentityContentEncoding;
    }

    return true;
}

bool TContext::TryAcquireConcurrencySemaphore()
{
    SemaphoreGuard_ = Api_->AcquireSemaphore(DriverRequest_.AuthenticatedUser, DriverRequest_.CommandName);
    if (!SemaphoreGuard_) {
        DispatchUnavailable(TError{
            NRpc::EErrorCode::RequestQueueSizeLimitExceeded,
            "There are too many concurrent requests being served at the moment; please try another proxy or try again later"});
        return false;
    }

    return true;
}

void TContext::CaptureParameters()
{
    DriverRequest_.Parameters = BuildYsonNodeFluently()
        .BeginMap()
            .Item("input_format").Value(InputFormat_)
            .Item("output_format").Value(OutputFormat_)
        .EndMap()->AsMap();

    try {
        auto queryParams = ParseQueryString(Request_->GetUrl().RawQuery);
        FixupNodesWithAttributes(queryParams);
        DriverRequest_.Parameters = PatchNode(DriverRequest_.Parameters, std::move(queryParams))->AsMap();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Unable to parse parameters from query string") << ex;
    }

    try {
        auto header = GatherHeader(Request_->GetHeaders(), "x-yt-parameters");
        if (header) {
            TMemoryInput stream(header->data(), header->size());
            auto fromHeaders = ConvertToNode(CreateProducerForFormat(
                *HeadersFormat_,
                EDataType::Structured,
                &stream));

            DriverRequest_.Parameters = PatchNode(DriverRequest_.Parameters, fromHeaders)->AsMap();
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Unable to parse parameters from headers") << ex;
    }

    if (Request_->GetMethod() == EMethod::Post) {
        auto body = Request_->ReadAll();
        if (body.Size() == 0) {
            return;
        }

        if (InputContentEncoding_ != IdentityContentEncoding) {
            THROW_ERROR_EXCEPTION("Content-Encoding not supported in POST body");
        }

        TMemoryInput stream(body.Begin(), body.Size());
        auto fromBody = ConvertToNode(CreateProducerForFormat(
            *InputFormat_,
            EDataType::Structured,
            &stream));

        DriverRequest_.Parameters = PatchNode(DriverRequest_.Parameters, fromBody)->AsMap();
    }
}

void TContext::SetETagRevision()
{
    auto etagHeader = Request_->GetHeaders()->Find("If-None-Match");
    if (etagHeader) {
        IfNoneMatch_.emplace();

        // COMPAT(shakurov)
        if (TryFromString(*etagHeader, IfNoneMatch_->Revision)) {
            DriverRequest_.Parameters->AsMap()->AddChild("etag_revision", ConvertToNode(*etagHeader));
        } else {
            IfNoneMatch_ = ParseEtag(*etagHeader)
                .ValueOrThrow();
            DriverRequest_.Parameters->AsMap()->AddChild("etag", ConvertToNode(*etagHeader));
        }
    }
}

void TContext::SetContentDispositionAndMimeType()
{
    TString disposition = "attachment";
    if (Descriptor_->Heavy) {
        TString filename;
        if (Descriptor_->CommandName == "download" ||
            Descriptor_->CommandName == "read_table" ||
            Descriptor_->CommandName == "read_file"
        ) {
            if (auto pathNode = DriverRequest_.Parameters->FindChild("path")) {
                filename = "yt_" + pathNode->GetValue<TString>();
            }
        } else if (Descriptor_->CommandName == "get_job_stderr") {
            auto operationIdNode = DriverRequest_.Parameters->FindChild("operation_id");
            auto jobIdNode = DriverRequest_.Parameters->FindChild("job_id");

            disposition = "inline";
            if (operationIdNode && jobIdNode) {
                filename = Format("job_stderr_%v_%v",
                    operationIdNode->GetValue<TString>(),
                    jobIdNode->GetValue<TString>());
            }
        } else if (Descriptor_->CommandName == "get_job_fail_context") {
            auto operationIdNode = DriverRequest_.Parameters->FindChild("operation_id");
            auto jobIdNode = DriverRequest_.Parameters->FindChild("job_id");

            disposition = "inline";
            if (operationIdNode && jobIdNode) {
                filename = Format("fail_context_%v_%v",
                    operationIdNode->GetValue<TString>(),
                    jobIdNode->GetValue<TString>());
            }
        }

        if (auto passedFilenameNode = DriverRequest_.Parameters->FindChild("filename")) {
            filename = passedFilenameNode->GetValue<TString>();
        }

        for (size_t i = 0; i < filename.size(); ++i) {
            if (!std::isalnum(filename[i]) && filename[i] != '.') {
                filename[i] = '_';
            }
        }

        if (filename.Contains("sys_operations") && filename.Contains("stderr")) {
            disposition = "inline";
        }

        if (auto passedDispositionNode = DriverRequest_.Parameters->FindChild("disposition")) {
            auto sanitizedDisposition = passedDispositionNode->GetValue<TString>();
            sanitizedDisposition.to_lower();
            if (sanitizedDisposition == "inline" && sanitizedDisposition == "attachment") {
                disposition = sanitizedDisposition;
            }
        }

        if (!filename.empty()) {
            disposition += "; filename=\"" + filename + "\"";
        }

        Response_->GetHeaders()->Set("Content-Disposition", disposition);
    }

    if (Descriptor_->OutputType == EDataType::Binary) {
        if (disposition.StartsWith("inline")) {
            ContentType_ = "text/plain; charset=\"utf-8\"";
        } else {
            ContentType_ = "application/octet-stream";
        }
    } else if (Descriptor_->OutputType == EDataType::Null) {
        return;
    } else {
        ContentType_ = FormatToMime(*OutputFormat_);
    }
}

void TContext::LogRequest()
{
    DriverRequest_.Id = Request_->GetRequestId();
    Parameters_ = ConvertToYsonString(
        HideSecretParameters(Descriptor_->CommandName, DriverRequest_.Parameters),
        EYsonFormat::Text).ToString();
    YT_LOG_INFO("Gathered request parameters (RequestId: %v, Command: %v, User: %v, Parameters: %v, InputFormat: %v, InputCompression: %v, OutputFormat: %v, OutputCompression: %v)",
        Request_->GetRequestId(),
        Descriptor_->CommandName,
        DriverRequest_.AuthenticatedUser,
        Parameters_,
        ConvertToYsonString(InputFormat_, EYsonFormat::Text).AsStringBuf(),
        InputContentEncoding_,
        ConvertToYsonString(OutputFormat_, EYsonFormat::Text).AsStringBuf(),
        OutputContentEncoding_);
}

void TContext::LogStructuredRequest()
{
    if (!PrepareFinished_) {
        return;
    }

    std::optional<TString> correlationId;
    if (auto correlationHeader = Request_->GetHeaders()->Find("X-YT-Correlation-ID")) {
        correlationId = *correlationHeader;
    }

    auto path = DriverRequest_.Parameters->AsMap()->FindChild("path");
    const auto* traceContext = NTracing::TryGetCurrentTraceContext();
    NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
        if (path && path->GetType() == ENodeType::String) {
            traceContext->AddTag("yt.table_path", path->AsString()->GetValue());
        }
    });

    YT_LOG_DEBUG("Request finished (RequestId: %v, Command: %v, User: %v, WallTime: %v, CpuTime: %v, InBytes: %v, OutBytes: %v)",
        Request_->GetRequestId(),
        Descriptor_->CommandName,
        DriverRequest_.AuthenticatedUser,
        WallTime_,
        CpuTime_,
        Request_->GetReadByteCount(),
        Response_->GetWriteByteCount());

    LogStructuredEventFluently(HttpStructuredProxyLogger, ELogLevel::Info)
        .Item("request_id").Value(Request_->GetRequestId())
        .Item("command").Value(Descriptor_->CommandName)
        .Item("user").Value(DriverRequest_.AuthenticatedUser)
        .DoIf(Auth_.has_value(), [&] (auto fluent) {
            fluent
                .Item("authenticated_from").Value(Auth_->Result.Login)
                .Item("token_hash").Value(Auth_->TokenHash);
        })
        .Item("parameters").Value(TYsonString{Parameters_})
        .Item("correlation_id").Value(correlationId)
        .DoIf(traceContext, [&] (auto fluent) {
            fluent
                .Item("trace_id").Value(traceContext->GetTraceId());
        })
        .Item("user_agent").Value(FindUserAgent(Request_))
        .Item("path").Value(path)
        .Item("http_path").Value(Request_->GetUrl().Path)
        .Item("method").Value(Request_->GetMethod())
        .Item("http_code").Value(static_cast<int>(Response_->GetStatus().value_or(EStatusCode::OK)))
        .Item("error_code").Value(static_cast<int>(Error_.GetCode()))
        .Item("error").Value(Error_)
        .Item("remote_address").Value(ToString(Request_->GetRemoteAddress()))
        .Item("l7_request_id").Value(FindBalancerRequestId(Request_))
        .Item("l7_real_ip").Value(FindBalancerRealIP(Request_))
        // COMPAT(babenko): rename to wall_time
        .Item("duration").Value(WallTime_)
        .Item("cpu_time").Value(CpuTime_)
        .Item("start_time").Value(Timer_.GetStartTime())
        .Item("in_bytes").Value(Request_->GetReadByteCount())
        .Item("out_bytes").Value(Response_->GetWriteByteCount());
}

void TContext::SetupInputStream()
{
    if (IdentityContentEncoding == InputContentEncoding_) {
        DriverRequest_.InputStream = CreateCopyingAdapter(Request_);
    } else {
        DriverRequest_.InputStream = CreateDecompressingAdapter(Request_, *InputContentEncoding_);
    }
}

void TContext::SetupOutputStream()
{
    if (!IsFramingEnabled_ && (
        Descriptor_->OutputType == EDataType::Null ||
        Descriptor_->OutputType == EDataType::Structured))
    {
        MemoryOutput_ = New<TSharedRefOutputStream>();
        DriverRequest_.OutputStream = MemoryOutput_;
    } else {
        DriverRequest_.OutputStream = Response_;
    }

    if (IdentityContentEncoding != OutputContentEncoding_) {
        DriverRequest_.OutputStream = CreateCompressingAdapter(
            DriverRequest_.OutputStream,
            *OutputContentEncoding_);
    }

    if (IsFramingEnabled_) {
        auto invoker = Api_->GetPoller()->GetInvoker();
        FramingStream_ = New<TFramingAsyncOutputStream>(DriverRequest_.OutputStream, invoker);
        DriverRequest_.OutputStream = FramingStream_;

        // NB: This lambda should not capture |this| (by strong reference) to avoid cyclic references.
        auto sendKeepAliveFrame = [Logger=Logger] (const TFramingAsyncOutputStreamPtr& stream) {
            // All errors are ignored.
            YT_LOG_DEBUG("Sending keep-alive frame");
            auto error = WaitFor(stream->WriteKeepAliveFrame());
            if (!error.IsOK()) {
                YT_LOG_ERROR("Error sending keep-alive frame", error);
                return;
            }
            Y_UNUSED(WaitFor(stream->Flush()));
        };

        if (auto keepAlivePeriod = GetFramingConfig()->KeepAlivePeriod; keepAlivePeriod) {
            YT_LOG_DEBUG("Creating periodic executor to send keep-alive frames (KeepAlivePeriod: %v)",
                *keepAlivePeriod);
            SendKeepAliveExecutor_ = New<TPeriodicExecutor>(
                invoker,
                BIND(sendKeepAliveFrame, FramingStream_),
                *keepAlivePeriod);
        }
    }
}

void TContext::SetupOutputParameters()
{
    CreateBuildingYsonConsumer(&OutputParametersConsumer_, EYsonType::Node);
    OutputParametersConsumer_->OnBeginMap();
    DriverRequest_.ResponseParametersConsumer = OutputParametersConsumer_.get();
    DriverRequest_.ResponseParametersFinishedCallback = [this, weakThis = MakeWeak(this)] {
        auto strongThis = weakThis.Lock();
        if (!strongThis) {
            return;
        }

        OutputParametersConsumer_->OnEndMap();
        OutputParameters_ = OutputParametersConsumer_->Finish()->AsMap();
        OnOutputParameters();

        if (SendKeepAliveExecutor_) {
            YT_LOG_DEBUG("Starting periodic executor to send keep-alive frames");
            SendKeepAliveExecutor_->Start();
        }

        ProcessDelayBeforeCommandTestingOption();
    };
}

void TContext::SetupTracing()
{
    if (auto* traceContext = TryGetCurrentTraceContext()) {
        if (Api_->GetConfig()->ForceTracing) {
            traceContext->SetSampled();
        }

        auto sampler = Api_->GetCoordinator()->GetTraceSampler();
        sampler->SampleTraceContext(DriverRequest_.AuthenticatedUser, traceContext);

        if (Api_->GetDynamicConfig()->EnableAllocationTags) {
            traceContext->SetAllocationTags({
                {HttpProxyUserAllocationTag, DriverRequest_.AuthenticatedUser},
                {HttpProxyRequestIdAllocationTag, ToString(Request_->GetRequestId())},
                {HttpProxyCommandAllocationTag, DriverRequest_.CommandName}
            });

            AllocateTestData(traceContext);
        }
    }
}

void TContext::AddHeaders()
{
    auto headers = Response_->GetHeaders();

    if (ContentType_) {
        headers->Set("Content-Type", *ContentType_);
    }

    if (OutputContentEncoding_) {
        headers->Set("Content-Encoding", *OutputContentEncoding_);
        headers->Add("Vary", "Content-Encoding");
    }

    if (!OmitTrailers_) {
        headers->Set("Trailer", "X-YT-Error, X-YT-Response-Code, X-YT-Response-Message");
    }
}

void TContext::SetEnrichedError(const TError& error)
{
    Error_ = error;
    // If request contains path (e.g. GetNode), enrich error with it.
    if (DriverRequest_.Parameters) {
        if (auto path = DriverRequest_.Parameters->FindChild("path")) {
            Error_ = Error_
                << TErrorAttribute("path", path->GetValue<TString>());
        }
    }
    YT_LOG_ERROR(Error_, "Command failed");
}

void TContext::ProcessFormatsInOperationSpec()
{
    auto specNode = DriverRequest_.Parameters->FindChild("spec");
    auto commandName = DriverRequest_.CommandName;
    auto operationTypeNode = DriverRequest_.Parameters->FindChild("operation_type");
    TString operationTypeString;
    if (commandName == "start_op" || commandName == "start_operation") {
        if (!operationTypeNode || operationTypeNode->GetType() != ENodeType::String) {
            return;
        }
        operationTypeString = operationTypeNode->GetValue<TString>();
    } else {
        operationTypeString = commandName;
    }

    EOperationType operationType;
    try {
        operationType = ParseEnum<EOperationType>(operationTypeString);
    } catch (const std::exception& error) {
        return;
    }

    FormatManager_->ValidateAndPatchOperationSpec(specNode, operationType);
}

void TContext::ProcessFormatsInParameters()
{
    ProcessFormatsInOperationSpec();

    if (auto inputFormatNode = DriverRequest_.Parameters->FindChild("input_format")) {
        FormatManager_->ValidateAndPatchFormatNode(inputFormatNode, "input_format");
    }
    if (auto outputFormatNode = DriverRequest_.Parameters->FindChild("output_format")) {
        FormatManager_->ValidateAndPatchFormatNode(outputFormatNode, "output_format");
    }
}

void TContext::FinishPrepare()
{
    CaptureParameters();
    ProcessFormatsInParameters();
    SetContentDispositionAndMimeType();
    SetETagRevision();
    LogRequest();
    SetupInputStream();
    SetupOutputStream();
    SetupOutputParameters();
    SetupTracing();
    AddHeaders();
    PrepareFinished_ = true;

    YT_LOG_DEBUG("Prepare finished");
}

void TContext::Run()
{
    Response_->SetStatus(EStatusCode::OK);

    auto driverRequest = DriverRequest_;
    if (driverRequest.CommandName == "discover_proxies") {
        driverRequest.AuthenticatedUser = NSecurityClient::RootUserName;
    }

    if (*ApiVersion_ == 4) {
        WaitFor(Api_->GetDriverV4()->Execute(driverRequest))
            .ThrowOnError();
    } else {
        WaitFor(Api_->GetDriverV3()->Execute(driverRequest))
            .ThrowOnError();
    }

    if (MemoryOutput_) {
        WaitFor(DriverRequest_.OutputStream->Close())
            .ThrowOnError();
        Response_->GetHeaders()->Remove("Trailer");
        WaitFor(Response_->WriteBody(MergeRefsToRef<TDefaultSharedBlobTag>(MemoryOutput_->GetRefs())))
            .ThrowOnError();
    } else {
        WaitFor(DriverRequest_.OutputStream->Close())
            .ThrowOnError();

        WaitFor(Response_->Close())
            .ThrowOnError();
    }
}

TSharedRef DumpError(const TError& error)
{
    TString delimiter;
    delimiter.append('\n');
    delimiter.append(80, '=');
    delimiter.append('\n');

    TString errorMessage;
    TStringOutput errorStream(errorMessage);
    errorStream << "\n";
    errorStream << delimiter;

    auto formatAttributes = CreateEphemeralAttributes();
    formatAttributes->SetYson("format", TYsonString(TStringBuf("pretty")));

    auto consumer = CreateConsumerForFormat(
        TFormat(EFormatType::Json, formatAttributes.Get()),
        EDataType::Structured,
        &errorStream);

    Serialize(error, consumer.get());
    consumer->Flush();

    errorStream << delimiter;
    errorStream.Finish();

    return TSharedRef::FromString(errorMessage);
}

void TContext::LogAndProfile()
{
    WallTime_ = Timer_.GetElapsedTime();
    if (const auto* traceContext = TryGetCurrentTraceContext()) {
        FlushCurrentTraceContextElapsedTime();
        CpuTime_ = traceContext->GetElapsedTime();
    }

    LogStructuredRequest();

    Api_->IncrementProfilingCounters(
        DriverRequest_.AuthenticatedUser,
        DriverRequest_.CommandName,
        Response_->GetStatus(),
        Error_.GetNonTrivialCode(),
        WallTime_,
        CpuTime_,
        Request_->GetRemoteAddress(),
        Request_->GetReadByteCount(),
        Response_->GetWriteByteCount(),
        InputFormat_,
        OutputFormat_,
        InputContentEncoding_,
        OutputContentEncoding_);
}

void TContext::Finalize()
{
    if (SendKeepAliveExecutor_) {
        YT_LOG_DEBUG("Stopping periodic executor that sends keep-alive frames");
        Y_UNUSED(WaitFor(SendKeepAliveExecutor_->Stop()));
    }

    if (EnableRequestBodyWorkaround(Request_)) {
        try {
            while (true) {
                auto chunk = WaitFor(Request_->Read())
                    .ValueOrThrow();

                if (!chunk || chunk.Empty()) {
                    break;
                }
            }
        } catch (const std::exception& ex) { }
    }

    bool dumpErrorIntoResponse = false;
    if (DriverRequest_.Parameters) {
        auto param = DriverRequest_.Parameters->FindChild("dump_error_into_response");
        if (param) {
            dumpErrorIntoResponse = ConvertTo<bool>(param);
        }
    }

    if (!Error_.IsOK() && dumpErrorIntoResponse && DriverRequest_.OutputStream) {
        Y_UNUSED(WaitFor(DriverRequest_.OutputStream->Write(DumpError(Error_))));
        Y_UNUSED(WaitFor(DriverRequest_.OutputStream->Close()));
        Y_UNUSED(WaitFor(Response_->Close()));
    } else if (!Response_->AreHeadersFlushed()) {
        Response_->GetHeaders()->Remove("Trailer");

        if (Error_.FindMatching(NSecurityClient::EErrorCode::UserBanned)) {
            Response_->SetStatus(EStatusCode::Forbidden);
            Api_->PutUserIntoBanCache(DriverRequest_.AuthenticatedUser);
        } else if (!Error_.IsOK()) {
            Response_->SetStatus(EStatusCode::BadRequest);
        }
        // TODO(prime@): More error codes.

        if (!Error_.IsOK()) {
            Response_->GetHeaders()->Remove("Content-Encoding");
            Response_->GetHeaders()->Remove("Vary");
            Response_->GetHeaders()->Remove("X-YT-Framing");

            FillYTErrorHeaders(Response_, Error_);
            DispatchJson([&] (auto producer) {
                BuildYsonFluently(producer).Value(Error_);
            });
        }
    } else {
        if (!Error_.IsOK()) {
            FillYTErrorTrailers(Response_, Error_);
            Y_UNUSED(WaitFor(Response_->Close()));
        }
    }
}

template <class TJsonProducer>
void TContext::DispatchJson(const TJsonProducer& producer)
{
    ReplyJson(Response_, [&] (NYson::IYsonConsumer* consumer) {
        producer(consumer);
    });
}

void TContext::DispatchUnauthorized(const TString& scope, const TString& message)
{
    Response_->SetStatus(EStatusCode::Unauthorized);
    Response_->GetHeaders()->Set("WWW-Authenticate", scope);
    ReplyError(TError{message});
}

void TContext::DispatchUnavailable(const TError& error)
{
    Response_->SetStatus(EStatusCode::ServiceUnavailable);
    // This header is header is probably useless, but we keep it for compatibility.
    Response_->GetHeaders()->Set("Retry-After", "60");
    ReplyError(error);
}

void TContext::DispatchNotFound(const TString& message)
{
    Response_->SetStatus(EStatusCode::NotFound);
    ReplyError(TError{message});
}

void TContext::ReplyError(const TError& error)
{
    YT_LOG_DEBUG(error, "Request finished with error");
    NHttpProxy::ReplyError(Response_, error);
}

void TContext::OnOutputParameters()
{
    auto idNode = OutputParameters_->FindChild("id");
    auto revisionNode = OutputParameters_->FindChild("revision");
    if (idNode && revisionNode) {
        TEtag etag{
            idNode->GetValue<TObjectId>(),
            revisionNode->GetValue<NHydra::TRevision>(),
        };
        Response_->GetHeaders()->Add("ETag", ToString(etag));
        if (IfNoneMatch_ && *IfNoneMatch_ == etag) {
            Response_->SetStatus(EStatusCode::NotModified);
        }
    }

    TString headerValue;
    TStringOutput stream(headerValue);
    auto consumer = CreateConsumerForFormat(*HeadersFormat_, EDataType::Structured, &stream);
    Serialize(OutputParameters_, consumer.get());
    consumer->Flush();

    if (auto userAgent = Request_->GetHeaders()->Find("User-Agent"); userAgent && DetectGo(*userAgent) == 0) {
        if (DriverRequest_.CommandName != "read_table") {
            return;
        }
    }

    Response_->GetHeaders()->Add("X-YT-Response-Parameters", headerValue);
}

TFramingConfigPtr TContext::GetFramingConfig() const
{
    return Api_->GetDynamicConfig()->Framing;
}

void TContext::ProcessDelayBeforeCommandTestingOption()
{
    auto testingOptions = Api_->GetConfig()->TestingOptions;
    if (!testingOptions) {
        return;
    }
    const auto& delayOptions = testingOptions->DelayBeforeCommand;
    if (delayOptions.empty()) {
        return;
    }
    auto it = delayOptions.find(DriverRequest_.CommandName);
    if (it == delayOptions.end()) {
        return;
    }
    const auto& commandDelayOptions = *it->second;
    auto node = FindNodeByYPath(DriverRequest_.Parameters, commandDelayOptions.ParameterPath);
    if (!node) {
        return;
    }
    auto nodeString = ConvertToYsonString(node, EYsonFormat::Text);
    if (nodeString.AsStringBuf().find(commandDelayOptions.Substring) == std::string::npos) {
        return;
    }
    YT_LOG_DEBUG("Waiting for %v seconds due to \"delay_before_command\" testing option",
        commandDelayOptions.Delay.SecondsFloat());
    TDelayedExecutor::WaitForDuration(commandDelayOptions.Delay);
}

void TContext::AllocateTestData(const TTraceContextPtr& traceContext)
{
    auto testingOptions = Api_->GetConfig()->TestingOptions;

    if (!testingOptions || !traceContext) {
        return;
    }

    auto heapProfilerOptions = testingOptions->HeapProfiler;

    if (heapProfilerOptions && heapProfilerOptions->AllocationSize) {
        auto guard = TCurrentTraceContextGuard(traceContext);

        auto size = heapProfilerOptions->AllocationSize.value();
        auto delay = heapProfilerOptions->AllocationReleaseDelay.value_or(TDuration::Zero());

        MakeTestHeapAllocation(size, delay);

        YT_LOG_DEBUG("Test heap allocation is finished (AllocationSize: %v, AllocationReleaseDelay: %v)",
            size,
            delay);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
