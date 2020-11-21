#include "http.h"

#include "abortable_http_response.h"
#include "mapreduce/yt/interface/errors.h"

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/retry_lib.h>
#include <mapreduce/yt/common/wait_proxy.h>

#include <mapreduce/yt/interface/logging/log.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/generic/singleton.h>

#include <util/stream/mem.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/escape.h>
#include <util/string/printf.h>
#include <util/system/getpid.h>

#include <exception>

namespace NYT {

static TString TruncateForLogs(const TString& text, size_t maxSize)
{
    Y_VERIFY(maxSize > 10);
    if (text.empty()) {
        static TString empty = "empty";
        return empty;
    } else if (text.size() > maxSize) {
        TStringStream out;
        out << text.substr(0, maxSize) + "... ("  << text.size() << " bytes total)";
        return out.Str();
    } else {
        return text;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TDebugRequestTracer
    : public IOutputStream
{
public:
    TDebugRequestTracer(IOutputStream* underlyingStream)
        : UnderlyingStream(underlyingStream)
    { }

    TStringBuf GetTrace() const
    {
        return Trace;
    }

private:
    void DoWrite(const void* buf, size_t len) override
    {
        const size_t saveToDebugLen = Min(len, MaxSize - Trace.size());
        UnderlyingStream->Write(buf, len);
        if (saveToDebugLen) {
            Trace.append(static_cast<const char*>(buf), saveToDebugLen);
        }
    }

    void DoFlush() override
    {
        UnderlyingStream->Flush();
    }

    void DoFinish() override
    {
        UnderlyingStream->Finish();
    }

private:
    static constexpr size_t MaxSize = 1024 * 1024;
    IOutputStream* const UnderlyingStream;
    TString Trace;
};


class THttpRequest::TRequestStream
    : public IOutputStream
{
public:
    TRequestStream(THttpRequest* httpRequest, const TSocket& s)
        : HttpRequest_(httpRequest)
        , SocketOutput_(s)
        , DebugTracer_(IsTracingRequired() ? MakeHolder<TDebugRequestTracer>(&SocketOutput_) : nullptr)
        , HttpOutput_(
                DebugTracer_
                ? static_cast<IOutputStream*>(DebugTracer_.Get())
                : static_cast<IOutputStream*>(&SocketOutput_))
    {
        HttpOutput_.EnableKeepAlive(true);
    }

    bool IsTracingEnabled() const
    {
        return DebugTracer_.Get();
    }

    TStringBuf GetTrace() const
    {
        if (DebugTracer_) {
            return DebugTracer_->GetTrace();
        }
        return {};
    }

private:
    void DoWrite(const void* buf, size_t len) override
    {
        WrapWriteFunc([&] {
            HttpOutput_.Write(buf, len);
        });
    }

    void DoWriteV(const TPart* parts, size_t count) override
    {
        WrapWriteFunc([&] {
            HttpOutput_.Write(parts, count);
        });
    }

    void DoWriteC(char ch) override
    {
        WrapWriteFunc([&] {
            HttpOutput_.Write(ch);
        });
    }

    void DoFlush() override
    {
        WrapWriteFunc([&] {
            HttpOutput_.Flush();
        });
    }

    void DoFinish() override
    {
        WrapWriteFunc([&] {
            HttpOutput_.Finish();
        });
    }

    void WrapWriteFunc(std::function<void()> func)
    {
        CheckErrorState();
        try {
            func();
        } catch (const yexception&) {
            HandleWriteException();
        }
    }

    // In many cases http proxy stops reading request and resets connection
    // if error has happend. This function tries to read error response
    // in such cases.
    void HandleWriteException() {
        Y_VERIFY(WriteError_ == nullptr);
        WriteError_ = std::current_exception();
        Y_VERIFY(WriteError_ != nullptr);
        try {
            HttpRequest_->GetResponseStream();
        } catch (const TErrorResponse &) {
            throw;
        } catch (...) {
        }
        std::rethrow_exception(WriteError_);
    }

    void CheckErrorState()
    {
        if (WriteError_) {
            std::rethrow_exception(WriteError_);
        }
    }

    static bool IsTracingRequired()
    {
        return TConfig::Get()->TraceHttpRequestsMode == ETraceHttpRequestsMode::Never;
    }

private:
    THttpRequest* const HttpRequest_;
    TSocketOutput SocketOutput_;
    THolder<TDebugRequestTracer> DebugTracer_;
    THttpOutput HttpOutput_;
    std::exception_ptr WriteError_;
};

////////////////////////////////////////////////////////////////////////////////

THttpHeader::THttpHeader(const TString& method, const TString& command, bool isApi)
    : Method(method)
    , Command(command)
    , IsApi(isApi)
{ }

void THttpHeader::AddParameter(const TString& key, TNode value, bool overwrite)
{
    auto it = Parameters.find(key);
    if (it == Parameters.end()) {
        Parameters.emplace(key, std::move(value));
    } else {
        if (overwrite) {
            it->second = std::move(value);
        } else {
            ythrow yexception() << "Duplicate key: " << key;
        }
    }
}

void THttpHeader::MergeParameters(const TNode& newParameters, bool overwrite)
{
    for (const auto& p : newParameters.AsMap()) {
        AddParameter(p.first, p.second, overwrite);
    }
}

void THttpHeader::RemoveParameter(const TString& key)
{
    Parameters.erase(key);
}

TNode THttpHeader::GetParameters() const
{
    return Parameters;
}

void THttpHeader::AddTransactionId(const TTransactionId& transactionId, bool overwrite)
{
    if (transactionId) {
        AddParameter("transaction_id", GetGuidAsString(transactionId), overwrite);
    } else {
        RemoveParameter("transaction_id");
    }
}

void THttpHeader::AddPath(const TString& path, bool overwrite)
{
    AddParameter("path", path, overwrite);
}

void THttpHeader::AddOperationId(const TOperationId& operationId, bool overwrite)
{
    AddParameter("operation_id", GetGuidAsString(operationId), overwrite);
}

void THttpHeader::AddMutationId()
{
    TGUID guid;

    // Some users use `fork()' with yt wrapper
    // (actually they use python + multiprocessing)
    // and CreateGuid is not resistant to `fork()', so spice it a little bit.
    //
    // Check IGNIETFERRO-610
    CreateGuid(&guid);
    guid.dw[2] = GetPID() ^ MicroSeconds();

    AddParameter("mutation_id", GetGuidAsString(guid), true);
}

bool THttpHeader::HasMutationId() const
{
    return Parameters.contains("mutation_id");
}

void THttpHeader::SetToken(const TString& token)
{
    Token = token;
}

void THttpHeader::SetInputFormat(const TMaybe<TFormat>& format)
{
    InputFormat = format;
}

void THttpHeader::SetOutputFormat(const TMaybe<TFormat>& format)
{
    OutputFormat = format;
}

TMaybe<TFormat> THttpHeader::GetOutputFormat() const
{
    return OutputFormat;
}

void THttpHeader::SetRequestCompression(const TString& compression)
{
    RequestCompression = compression;
}

void THttpHeader::SetResponseCompression(const TString& compression)
{
    ResponseCompression = compression;
}

TString THttpHeader::GetCommand() const
{
    return Command;
}

TString THttpHeader::GetUrl() const
{
    TStringStream url;

    if (IsApi) {
        url << "/api/" << TConfig::Get()->ApiVersion << "/" << Command;
    } else {
        url << "/" << Command;
    }

    return url.Str();
}

TString THttpHeader::GetHeader(const TString& hostName, const TString& requestId, bool includeParameters) const
{
    TStringStream header;

    header << Method << " " << GetUrl() << " HTTP/1.1\r\n";
    header << "Host: " << hostName << "\r\n";
    header << "User-Agent: " << TProcessState::Get()->ClientVersion << "\r\n";

    if (!Token.empty()) {
        header << "Authorization: OAuth " << Token << "\r\n";
    }

    if (Method == "PUT" || Method == "POST") {
        header << "Transfer-Encoding: chunked\r\n";
    }

    header << "X-YT-Correlation-Id: " << requestId << "\r\n";
    header << "X-YT-Header-Format: <format=text>yson\r\n";

    header << "Content-Encoding: " << RequestCompression << "\r\n";
    header << "Accept-Encoding: " << ResponseCompression << "\r\n";

    auto printYTHeader = [&header] (const char* headerName, const TString& value) {
        static const size_t maxHttpHeaderSize = 64 << 10;
        if (!value) {
            return;
        }
        if (value.size() <= maxHttpHeaderSize) {
            header << headerName << ": " << value << "\r\n";
            return;
        }

        TString encoded;
        Base64Encode(value, encoded);
        auto ptr = encoded.data();
        auto finish = encoded.data() + encoded.size();
        size_t index = 0;
        do {
            auto end = Min(ptr + maxHttpHeaderSize, finish);
            header << headerName << index++ << ": " <<
                TStringBuf(ptr, end) << "\r\n";
            ptr = end;
        } while (ptr != finish);
    };

    if (InputFormat) {
        printYTHeader("X-YT-Input-Format", NodeToYsonString(InputFormat->Config));
    }
    if (OutputFormat) {
        printYTHeader("X-YT-Output-Format", NodeToYsonString(OutputFormat->Config));
    }
    if (includeParameters) {
        printYTHeader("X-YT-Parameters", NodeToYsonString(Parameters));
    }

    header << "\r\n";
    return header.Str();
}

const TString& THttpHeader::GetMethod() const
{
    return Method;
}

////////////////////////////////////////////////////////////////////////////////

TAddressCache* TAddressCache::Get()
{
    return Singleton<TAddressCache>();
}

bool ContainsAddressOfRequiredVersion(const TAddressCache::TAddressPtr& address)
{
    if (!TConfig::Get()->ForceIpV4 && !TConfig::Get()->ForceIpV6) {
        return true;
    }

    for (auto i = address->Begin(); i != address->End(); ++i) {
        const auto& addressInfo = *i;
        if (TConfig::Get()->ForceIpV4 && addressInfo.ai_family == AF_INET) {
            return true;
        }
        if (TConfig::Get()->ForceIpV6 && addressInfo.ai_family == AF_INET6) {
            return true;
        }
    }
    return false;
}

TAddressCache::TAddressPtr TAddressCache::Resolve(const TString& hostName)
{
    TAddressPtr entry;
    if (TReadGuard guard(Lock_); auto* entryPtr = Cache_.FindPtr(hostName)) {
        entry = *entryPtr;
    }
    if (entry) {
        if (ContainsAddressOfRequiredVersion(entry)) {
            return entry;
        } else {
            LOG_DEBUG("Address of required version not found for host %s, will retry resolution",
                hostName.data());
        }
    }

    TString host(hostName);
    ui16 port = 80;

    auto colon = hostName.find(':');
    if (colon != TString::npos) {
        port = FromString<ui16>(hostName.substr(colon + 1));
        host = hostName.substr(0, colon);
    }

    auto retryPolicy = CreateDefaultRequestRetryPolicy();
    auto error = yexception() << "can not resolve address of required version for host " << hostName;
    while (true) {
        entry = new TNetworkAddress(host, port);
        if (ContainsAddressOfRequiredVersion(entry)) {
            break;
        }
        retryPolicy->NotifyNewAttempt();
        LOG_DEBUG("Failed to resolve address of required version for host %s, retrying: %s",
            hostName.data(),
            retryPolicy->GetAttemptDescription().data());
        if (auto backoffDuration = retryPolicy->OnGenericError(error)) {
            NDetail::TWaitProxy::Get()->Sleep(*backoffDuration);
        } else {
            ythrow error;
        }
    }

    TWriteGuard guard(Lock_);
    Cache_.insert({hostName, entry});
    return entry;
}

////////////////////////////////////////////////////////////////////////////////

TConnectionPool* TConnectionPool::Get()
{
    return Singleton<TConnectionPool>();
}

TConnectionPtr TConnectionPool::Connect(
    const TString& hostName,
    TDuration socketTimeout)
{
    Refresh();

    if (socketTimeout == TDuration::Zero()) {
        socketTimeout = TConfig::Get()->SocketTimeout;
    }

    {
        auto guard = Guard(Lock_);
        auto now = TInstant::Now();
        auto range = Connections_.equal_range(hostName);
        for (auto it = range.first; it != range.second; ++it) {
            auto& connection = it->second;
            if (connection->DeadLine < now) {
                continue;
            }
            if (!AtomicCas(&connection->Busy, 1, 0)) {
                continue;
            }

            connection->DeadLine = now + socketTimeout;
            connection->Socket->SetSocketTimeout(socketTimeout.Seconds());
            return connection;
        }
    }

    TConnectionPtr connection(new TConnection);

    auto networkAddress = TAddressCache::Get()->Resolve(hostName);
    TSocketHolder socket(DoConnect(networkAddress));
    SetNonBlock(socket, false);

    connection->Socket.Reset(new TSocket(socket.Release()));

    connection->DeadLine = TInstant::Now() + socketTimeout;
    connection->Socket->SetSocketTimeout(socketTimeout.Seconds());

    {
        auto guard = Guard(Lock_);
        static ui32 connectionId = 0;
        connection->Id = ++connectionId;
        Connections_.insert({hostName, connection});
    }

    LOG_DEBUG("New connection to %s #%u opened",
        hostName.c_str(),
        connection->Id);

    return connection;
}

void TConnectionPool::Release(TConnectionPtr connection)
{
    auto socketTimeout = TConfig::Get()->SocketTimeout;
    auto newDeadline = TInstant::Now() + socketTimeout;

    {
        auto guard = Guard(Lock_);
        connection->DeadLine = newDeadline;
    }

    connection->Socket->SetSocketTimeout(socketTimeout.Seconds());
    AtomicSet(connection->Busy, 0);

    Refresh();
}

void TConnectionPool::Invalidate(
    const TString& hostName,
    TConnectionPtr connection)
{
    auto guard = Guard(Lock_);
    auto range = Connections_.equal_range(hostName);
    for (auto it = range.first; it != range.second; ++it) {
        if (it->second == connection) {
            LOG_DEBUG("Closing connection #%u",
                connection->Id);
            Connections_.erase(it);
            return;
        }
    }
}

void TConnectionPool::Refresh()
{
    auto guard = Guard(Lock_);

    // simple, since we don't expect too many connections
    using TItem = std::pair<TInstant, TConnectionMap::iterator>;
    std::vector<TItem> sortedConnections;
    for (auto it = Connections_.begin(); it != Connections_.end(); ++it) {
        sortedConnections.emplace_back(it->second->DeadLine, it);
    }

    std::sort(
        sortedConnections.begin(),
        sortedConnections.end(),
        [] (const TItem& a, const TItem& b) -> bool {
            return a.first < b.first;
        });

    auto removeCount = static_cast<int>(Connections_.size()) - TConfig::Get()->ConnectionPoolSize;

    const auto now = TInstant::Now();
    for (const auto& item : sortedConnections) {
        const auto& mapIterator = item.second;
        auto connection = mapIterator->second;
        if (AtomicGet(connection->Busy)) {
            continue;
        }

        if (removeCount > 0) {
            Connections_.erase(mapIterator);
            LOG_DEBUG("Closing connection #%u (too many opened connections)",
                connection->Id);
            --removeCount;
            continue;
        }

        if (connection->DeadLine < now) {
            Connections_.erase(mapIterator);
            LOG_DEBUG("Closing connection #%u (timeout)",
                connection->Id);
        }
    }
}

SOCKET TConnectionPool::DoConnect(TAddressCache::TAddressPtr address)
{
    int lastError = 0;

    for (auto i = address->Begin(); i != address->End(); ++i) {
        struct addrinfo* info = &*i;

        if (TConfig::Get()->ForceIpV4 && info->ai_family != AF_INET) {
            continue;
        }

        if (TConfig::Get()->ForceIpV6 && info->ai_family != AF_INET6) {
            continue;
        }

        TSocketHolder socket(
            ::socket(info->ai_family, info->ai_socktype, info->ai_protocol));

        if (socket.Closed()) {
            lastError = LastSystemError();
            continue;
        }

        SetNonBlock(socket, true);

        if (connect(socket, info->ai_addr, info->ai_addrlen) == 0)
            return socket.Release();

        int err = LastSystemError();
        if (err == EINPROGRESS || err == EAGAIN || err == EWOULDBLOCK) {
            struct pollfd p = {
                socket,
                POLLOUT,
                0
            };
            const ssize_t n = PollD(&p, 1, TInstant::Now() + TConfig::Get()->ConnectTimeout);
            if (n < 0) {
                ythrow TSystemError(-(int)n) << "can not connect to " << info;
            }
            CheckedGetSockOpt(socket, SOL_SOCKET, SO_ERROR, err, "socket error");
            if (!err)
                return socket.Release();
        }

        lastError = err;
        continue;
    }

    ythrow TSystemError(lastError) << "can not connect to " << *address;
}

////////////////////////////////////////////////////////////////////////////////

static TMaybe<TString> GetProxyName(const THttpInput& input)
{
    if (auto proxyHeader = input.Headers().FindHeader("X-YT-Proxy")) {
        return proxyHeader->Value();
    }
    return Nothing();
}

THttpResponse::THttpResponse(
    IInputStream* socketStream,
    const TString& requestId,
    const TString& hostName)
    : HttpInput_(socketStream)
    , RequestId_(requestId)
    , HostName_(GetProxyName(HttpInput_).GetOrElse(hostName))
{
    HttpCode_ = ParseHttpRetCode(HttpInput_.FirstLine());
    if (HttpCode_ == 200 || HttpCode_ == 202) {
        return;
    }

    ErrorResponse_ = TErrorResponse(HttpCode_, RequestId_);

    auto logAndSetError = [&] (const TString& rawError) {
        LOG_ERROR("RSP %s - HTTP %d - %s",
            RequestId_.data(),
            HttpCode_,
            rawError.data());
        ErrorResponse_->SetRawError(rawError);
    };

    switch (HttpCode_) {
        case 429:
            logAndSetError("request rate limit exceeded");
            break;

        case 500:
            logAndSetError(TStringBuilder() << "internal error in proxy " << HostName_);
            break;

        default: {
            TStringStream httpHeaders;
            httpHeaders << "HTTP headers (";
            for (const auto& header : HttpInput_.Headers()) {
                httpHeaders << header.Name() << ": " << header.Value() << "; ";
            }
            httpHeaders << ")";

            auto errorString = Sprintf("RSP %s - HTTP %d - %s",
                RequestId_.data(),
                HttpCode_,
                httpHeaders.Str().data());

            LOG_ERROR("%s", errorString.data());

            if (auto parsedResponse = ParseError(HttpInput_.Headers())) {
                ErrorResponse_ = parsedResponse.GetRef();
            } else {
                ErrorResponse_->SetRawError(
                    errorString + " - X-YT-Error is missing in headers");
            }
            break;
        }
    }
}

const THttpHeaders& THttpResponse::Headers() const
{
    return HttpInput_.Headers();
}

void THttpResponse::CheckErrorResponse() const
{
    if (ErrorResponse_) {
        throw *ErrorResponse_;
    }
}

bool THttpResponse::IsExhausted() const
{
    return IsExhausted_;
}

int THttpResponse::GetHttpCode() const
{
    return HttpCode_;
}

const TString& THttpResponse::GetHostName() const
{
    return HostName_;
}

bool THttpResponse::IsKeepAlive() const
{
    return HttpInput_.IsKeepAlive();
}

TMaybe<TErrorResponse> THttpResponse::ParseError(const THttpHeaders& headers)
{
    for (const auto& header : headers) {
        if (header.Name() == "X-YT-Error") {
            TErrorResponse errorResponse(HttpCode_, RequestId_);
            errorResponse.ParseFromJsonError(header.Value());
            if (errorResponse.IsOk()) {
                return Nothing();
            }
            return errorResponse;
        }
    }
    return Nothing();
}

size_t THttpResponse::DoRead(void* buf, size_t len)
{
    size_t read = HttpInput_.Read(buf, len);
    if (read == 0 && len != 0) {
        // THttpInput MUST return defined (but may be empty)
        // trailers when it is exhausted.
        Y_VERIFY(HttpInput_.Trailers().Defined(),
            "trailers MUST be defined for exhausted stream");
        CheckTrailers(HttpInput_.Trailers().GetRef());
        IsExhausted_ = true;
    }
    return read;
}

size_t THttpResponse::DoSkip(size_t len)
{
    size_t skipped = HttpInput_.Skip(len);
    if (skipped == 0 && len != 0) {
        // THttpInput MUST return defined (but may be empty)
        // trailers when it is exhausted.
        Y_VERIFY(HttpInput_.Trailers().Defined(),
            "trailers MUST be defined for exhausted stream");
        CheckTrailers(HttpInput_.Trailers().GetRef());
        IsExhausted_ = true;
    }
    return skipped;
}

void THttpResponse::CheckTrailers(const THttpHeaders& trailers)
{
    if (auto errorResponse = ParseError(trailers)) {
        errorResponse->SetIsFromTrailers(true);
        LOG_ERROR("RSP %s - %s",
            RequestId_.data(),
            errorResponse.GetRef().what());
        ythrow errorResponse.GetRef();
    }
}

////////////////////////////////////////////////////////////////////////////////

THttpRequest::THttpRequest()
{
    RequestId = CreateGuidAsString();
}

THttpRequest::~THttpRequest()
{
    if (!Connection) {
        return;
    }

    if (Input && Input->IsKeepAlive() && Input->IsExhausted()) {
        // We should return to the pool only connections where HTTP response was fully read.
        // Otherwise next reader might read our remaining data and misinterpret them (YT-6510).
        TConnectionPool::Get()->Release(Connection);
    } else {
        TConnectionPool::Get()->Invalidate(HostName, Connection);
    }
}

TString THttpRequest::GetRequestId() const
{
    return RequestId;
}

void THttpRequest::Connect(TString hostName, TDuration socketTimeout)
{
    HostName = std::move(hostName);
    LOG_DEBUG("REQ %s - requesting connection to %s from connection pool",
        RequestId.data(),
        HostName.data());

    StartTime_ = TInstant::Now();
    Connection = TConnectionPool::Get()->Connect(HostName, socketTimeout);

    LOG_DEBUG("REQ %s - connection #%u",
        RequestId.data(),
        Connection->Id);
}

static TString GetParametersDebugString(const THttpHeader& header)
{
    const auto& parameters = header.GetParameters();
    if (parameters.Empty()) {
        return "<empty>";
    } else {
        return NodeToYsonString(parameters);
    }
}

IOutputStream* THttpRequest::StartRequestImpl(const THttpHeader& header, bool includeParameters)
{
    auto strHeader = header.GetHeader(HostName, RequestId, includeParameters);
    Url_ = header.GetUrl();

    const auto parametersDebugString = GetParametersDebugString(header);
    auto getLoggedAttributes = [&] (size_t sizeLimit) {
        TStringStream out;
        out << "Method: " << Url_ << "; "
            << "X-YT-Parameters (sent in " << (includeParameters ? "header" : "body") << "): " << TruncateForLogs(parametersDebugString, sizeLimit);
        return out.Str();
    };
    LOG_DEBUG("REQ %s - sending request (HostName: %s; %s)",
        RequestId.data(),
        HostName.c_str(),
        getLoggedAttributes(Max<size_t>()).c_str());

    LoggedAttributes_ = getLoggedAttributes(128);

    auto outputFormat = header.GetOutputFormat();
    if (outputFormat && outputFormat->IsTextYson()) {
        LogResponse = true;
    }

    RequestStream_ = MakeHolder<TRequestStream>(this, *Connection->Socket.Get());

    RequestStream_->Write(strHeader.data(), strHeader.size());
    return RequestStream_.Get();
}

IOutputStream* THttpRequest::StartRequest(const THttpHeader& header)
{
    return StartRequestImpl(header, true);
}

void THttpRequest::FinishRequest()
{
    RequestStream_->Flush();
    RequestStream_->Finish();
    if (TConfig::Get()->TraceHttpRequestsMode == ETraceHttpRequestsMode::Always) {
        TraceRequest(*this);
    }
}

void THttpRequest::SmallRequest(const THttpHeader& header, TMaybe<TStringBuf> body)
{
    if (!body && (header.GetMethod() == "PUT" || header.GetMethod() == "POST")) {
        const auto& parameters = header.GetParameters();
        auto parametersStr = NodeToYsonString(parameters);
        auto* output = StartRequestImpl(header, false);
        output->Write(parametersStr);
        FinishRequest();
    } else {
        auto* output = StartRequest(header);
        if (body) {
            output->Write(*body);
        }
        FinishRequest();
    }
}

THttpResponse* THttpRequest::GetResponseStream()
{
    if (!Input) {
        SocketInput.Reset(new TSocketInput(*Connection->Socket.Get()));
        if (TConfig::Get()->UseAbortableResponse) {
            Y_VERIFY(!Url_.empty());
            Input.Reset(new TAbortableHttpResponse(SocketInput.Get(), RequestId, HostName, Url_));
        } else {
            Input.Reset(new THttpResponse(SocketInput.Get(), RequestId, HostName));
        }
        Input->CheckErrorResponse();
    }
    return Input.Get();
}

TString THttpRequest::GetResponse()
{
    TString result = GetResponseStream()->ReadAll();

    TStringStream loggedAttributes;
    loggedAttributes
        << "Time: " << TInstant::Now() - StartTime_ << "; "
        << "HostName: " << GetResponseStream()->GetHostName() << "; "
        << LoggedAttributes_;

    if (LogResponse) {
        constexpr auto sizeLimit = 1 << 7;
        LOG_DEBUG("RSP %s - received response (Response: '%s'; %s)",
            RequestId.c_str(),
            TruncateForLogs(result, sizeLimit).c_str(),
            loggedAttributes.Str().c_str());
    } else {
        LOG_DEBUG("RSP %s - received response of %" PRISZT " bytes (%s)",
            RequestId.data(),
            result.size(),
            loggedAttributes.Str().c_str());
    }
    return result;
}

int THttpRequest::GetHttpCode() {
    return GetResponseStream()->GetHttpCode();
}

void THttpRequest::InvalidateConnection()
{
    TConnectionPool::Get()->Invalidate(HostName, Connection);
    Connection.Reset();
}

TString THttpRequest::GetTracedHttpRequest() const
{
    if (!RequestStream_->IsTracingEnabled()) {
        return {};
    }
    TStringStream result;
    TMemoryInput savedRequest(RequestStream_->GetTrace());
    TString line;
    while (savedRequest.ReadLine(line)) {
        TStringBuf authPattern = "Authorization: OAuth";
        if (line.StartsWith(authPattern)) {
            for (size_t i = authPattern.size(); i < line.size(); ++i) {
                if (!isspace(line[i])) {
                    line[i] = '*';
                }
            }
        }

        result << ">   " << EscapeC(line) << Endl;
    }

    return result.Str();
}

////////////////////////////////////////////////////////////////////////////////

void TraceRequest(const THttpRequest& request)
{
    Y_VERIFY(TConfig::Get()->TraceHttpRequestsMode == ETraceHttpRequestsMode::Error ||
             TConfig::Get()->TraceHttpRequestsMode == ETraceHttpRequestsMode::Always);
    auto httpRequestTrace = request.GetTracedHttpRequest();
    LOG_DEBUG("Dump of request %s:\n%s\n",
        request.GetRequestId().data(),
        httpRequestTrace.data()
    );
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
