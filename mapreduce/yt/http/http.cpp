#include "http.h"

#include "abortable_http_response.h"
#include "error.h"

#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/config.h>

#include <library/json/json_writer.h>
#include <library/string_utils/base64/base64.h>

#include <util/string/quote.h>
#include <util/string/printf.h>
#include <util/string/cast.h>
#include <util/string/builder.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

THttpHeader::THttpHeader(const Stroka& method, const Stroka& command, bool isApi)
    : Method(method)
    , Command(command)
    , IsApi(isApi)
{ }

void THttpHeader::AddParam(const Stroka& key, const char* value)
{
    Params[key] = value;
}

void THttpHeader::AddParam(const Stroka& key, const Stroka& value)
{
    Params[key] = value;
}

void THttpHeader::AddParam(const Stroka& key, i64 value)
{
    Params[key] = Sprintf("%" PRIi64, value);
}

void THttpHeader::AddParam(const Stroka& key, ui64 value)
{
    Params[key] = Sprintf("%" PRIu64, value);
}

void THttpHeader::AddParam(const Stroka& key, bool value)
{
    Params[key] = value ? "true" : "false";
}

void THttpHeader::RemoveParam(const Stroka& key)
{
    Params.erase(key);
}

void THttpHeader::AddTransactionId(const TTransactionId& transactionId)
{
    if (transactionId) {
        AddParam("transaction_id", GetGuidAsString(transactionId));
    } else {
        RemoveParam("transaction_id");
    }
}

void THttpHeader::AddPath(const Stroka& path)
{
    AddParam("path", path);
}

void THttpHeader::AddOperationId(const TOperationId& operationId)
{
    AddParam("operation_id", GetGuidAsString(operationId));
}

void THttpHeader::AddMutationId()
{
    AddParam("mutation_id", CreateGuidAsString());
}

void THttpHeader::SetToken(const Stroka& token)
{
    Token = token;
}

void THttpHeader::SetDataStreamFormat(EDataStreamFormat format)
{
    DataStreamFormat = format;
}

EDataStreamFormat THttpHeader::GetDataStreamFormat() const
{
    return DataStreamFormat;
}

void THttpHeader::SetInputFormat(const Stroka& format)
{
    InputFormat = format;
}

void THttpHeader::SetOutputFormat(const Stroka& format)
{
    OutputFormat = format;
}

void THttpHeader::SetParameters(const Stroka& parameters)
{
    Parameters = parameters;
}

Stroka THttpHeader::GetParameters() const
{
    return Parameters;
}

void THttpHeader::SetRequestCompression(const Stroka& compression)
{
    RequestCompression = compression;
}

void THttpHeader::SetResponseCompression(const Stroka& compression)
{
    ResponseCompression = compression;
}

Stroka THttpHeader::GetCommand() const
{
    return Command;
}

Stroka THttpHeader::GetUrl() const
{
    TStringStream url;

    if (IsApi) {
        url << "/api/" << TConfig::Get()->ApiVersion << "/" << Command;
    } else {
        url << "/" << Command;
    }

    if (!Params.empty()) {
        url << "?";
        bool first = true;
        for (const auto& p : Params) {
            if (!first) {
                url << "&";
            }
            url << p.first << "=" << CGIEscapeRet(p.second);
            first = false;
        }
    }

    return url.Str();
}

Stroka THttpHeader::GetHeader(const Stroka& hostName, const Stroka& requestId) const
{
    TStringStream header;

    header << Method << " " << GetUrl() << " HTTP/1.1\r\n";
    header << "Host: " << hostName << "\r\n";
    header << "User-Agent: " << TProcessState::Get()->ClientVersion << "\r\n";

    if (!Token.Empty()) {
        header << "Authorization: OAuth " << Token << "\r\n";
    }

    if (Method == "PUT" || Method == "POST") {
        header << "Transfer-Encoding: chunked\r\n";
    }

    header << "X-YT-Correlation-Id: " << requestId << "\r\n";
    header << "X-YT-Header-Format: <format=text>yson\r\n";

    header << "Content-Encoding: " << RequestCompression << "\r\n";
    header << "Accept-Encoding: " << ResponseCompression << "\r\n";

    switch (DataStreamFormat) {
        case DSF_YSON_BINARY:
            header << "Accept: application/x-yt-yson-binary\r\n";
            header << "Content-Type: application/x-yt-yson-binary\r\n";
            break;
        case DSF_YSON_TEXT:
            header << "Accept: application/x-yt-yson-text\r\n";
            header << "Content-Type: application/x-yt-yson-text\r\n";
            break;
        case DSF_YAMR_LENVAL:
            header << "Accept: application/x-yamr-subkey-lenval\r\n";
            header << "Content-Type: application/x-yamr-subkey-lenval\r\n";
            break;
        case DSF_BYTES:
            header << "Accept: application/octet-stream\r\n";
            header << "Content-Type: application/octet-stream\r\n";
        default:
            break;
    }

    auto printYTHeader = [&header] (const char* headerName, const Stroka& value) {
        static const size_t maxHttpHeaderSize = 64 << 10;
        if (!value) {
            return;
        }
        if (value.Size() <= maxHttpHeaderSize) {
            header << headerName << ": " << value << "\r\n";
            return;
        }

        Stroka encoded;
        Base64Encode(value, encoded);
        auto ptr = encoded.Data();
        auto finish = encoded.Data() + encoded.Size();
        size_t index = 0;
        do {
            auto end = Min(ptr + maxHttpHeaderSize, finish);
            header << headerName << index++ << ": " <<
                TStringBuf(ptr, end) << "\r\n";
            ptr = end;
        } while (ptr != finish);
    };

    printYTHeader("X-YT-Input-Format", InputFormat);
    printYTHeader("X-YT-Output-Format", OutputFormat);
    printYTHeader("X-YT-Parameters", Parameters);

    header << "\r\n";
    return header.Str();
}

////////////////////////////////////////////////////////////////////////////////

TAddressCache* TAddressCache::Get()
{
    return Singleton<TAddressCache>();
}

TAddressCache::TAddressPtr TAddressCache::Resolve(const Stroka& hostName)
{
    {
        TReadGuard guard(Lock_);
        if (auto* entry = Cache_.FindPtr(hostName)) {
            return *entry;
        }
    }

    Stroka host(hostName);
    ui16 port = 80;

    auto colon = hostName.find(':');
    if (colon != Stroka::npos) {
        port = FromString<ui16>(hostName.substr(colon + 1));
        host = hostName.substr(0, colon);
    }

    TAddressPtr entry = new TNetworkAddress(host, port);

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
    const Stroka& hostName,
    TDuration socketTimeout)
{
    Refresh();

    if (socketTimeout == TDuration::Zero()) {
        socketTimeout = TConfig::Get()->SocketTimeout;
    }

    {
        TReadGuard guard(Lock_);
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
    {
        TWriteGuard guard(Lock_);
        Connections_.insert({hostName, connection});
        static ui32 connectionId = 0;
        connection->Id = ++connectionId;
    }

    auto networkAddress = TAddressCache::Get()->Resolve(hostName);
    TSocketHolder socket(DoConnect(networkAddress));
    SetNonBlock(socket, false);

    connection->Socket.Reset(new TSocket(socket.Release()));

    connection->DeadLine = TInstant::Now() + socketTimeout;
    connection->Socket->SetSocketTimeout(socketTimeout.Seconds());

    LOG_DEBUG("Connection #%u opened",
        connection->Id);

    return connection;
}

void TConnectionPool::Release(TConnectionPtr connection)
{
    auto socketTimeout = TConfig::Get()->SocketTimeout;
    connection->DeadLine = TInstant::Now() + socketTimeout;
    connection->Socket->SetSocketTimeout(socketTimeout.Seconds());
    AtomicSet(connection->Busy, 0);

    Refresh();
}

void TConnectionPool::Invalidate(
    const Stroka& hostName,
    TConnectionPtr connection)
{
    TWriteGuard guard(Lock_);
    auto range = Connections_.equal_range(hostName);
    for (auto it = range.first; it != range.second; ++it) {
        if (it->second == connection) {
            LOG_DEBUG("Connection #%u invalidated",
                connection->Id);
            Connections_.erase(it);
            return;
        }
    }
}

void TConnectionPool::Refresh()
{
    TWriteGuard guard(Lock_);

    // simple, since we don't expect too many connections
    using TItem = std::pair<TInstant, TConnectionMap::iterator>;
    std::vector<TItem> sortedConnections;
    for (auto it = Connections_.begin(); it != Connections_.end(); ++it) {
        // We save DeadLine here cause it can be changed (from Connect) during our sorting.
        // TODO: we access DeadLine in nonatomic way
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
            LOG_DEBUG("Connection #%u closed",
                connection->Id);
            --removeCount;
            continue;
        }

        if (connection->DeadLine < now) {
            Connections_.erase(mapIterator);
            LOG_DEBUG("Connection #%u closed (timeout)",
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

THttpResponse::THttpResponse(
    TInputStream* socketStream,
    const Stroka& requestId,
    const Stroka& hostName)
    : HttpInput_(socketStream)
    , RequestId_(requestId)
    , HostName_(hostName)
{
    HttpCode_ = ParseHttpRetCode(HttpInput_.FirstLine());
    if (HttpCode_ == 200 || HttpCode_ == 202) {
        return;
    }

    ErrorResponse_ = TErrorResponse(HttpCode_, RequestId_);

    auto logAndSetError = [&] (const Stroka& rawError) {
        LOG_ERROR("RSP %s - HTTP %d - %s",
            ~RequestId_,
            HttpCode_,
            ~rawError);
        ErrorResponse_->SetRawError(rawError);
    };

    switch (HttpCode_) {
        case 401:
            logAndSetError("authentication error");
            break;

        case 429:
            logAndSetError("request rate limit exceeded");
            break;

        case 500:
            logAndSetError(TStringBuilder() << "internal error in proxy " << HostName_);
            break;

        case 503:
            logAndSetError(TStringBuilder() << "proxy " << HostName_ << " is unavailable");
            break;

        default: {
            TStringStream httpHeaders;
            httpHeaders << "HTTP headers (";
            for (const auto& header : HttpInput_.Headers()) {
                httpHeaders << header.Name() << ": " << header.Value() << "; ";
            }
            httpHeaders << ")";

            auto errorString = Sprintf("RSP %s - HTTP %d - %s",
                ~RequestId_,
                HttpCode_,
                ~httpHeaders.Str());

            LOG_ERROR(~errorString);

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
        LOG_ERROR("RSP %s - %s",
            ~RequestId_,
            errorResponse.GetRef().what());
        ythrow errorResponse.GetRef();
    }
}

////////////////////////////////////////////////////////////////////////////////

THttpRequest::THttpRequest(const Stroka& hostName)
    : HostName(hostName)
{
    RequestId = CreateGuidAsString();
}

THttpRequest::~THttpRequest()
{
    if (!Connection) {
        return;
    }

    bool keepAlive = false;
    if (Input) {
        for (const auto& header : Input->Headers()) {
            if (header.Name() == "Connection" && to_lower(header.Value()) == "keep-alive") {
                keepAlive = true;
                break;
            }
        }
    }

    if (keepAlive && Input && Input->IsExhausted()) {
        // We should return to the pool only connections were HTTP response was fully read.
        // Otherwise next reader might read our remaining data and misinterpret them (YT-6510).
        TConnectionPool::Get()->Release(Connection);
    } else {
        TConnectionPool::Get()->Invalidate(HostName, Connection);
    }
}

Stroka THttpRequest::GetRequestId() const
{
    return RequestId;
}

void THttpRequest::Connect(TDuration socketTimeout)
{
    LOG_DEBUG("REQ %s - connect to %s",
        ~RequestId,
        ~HostName);

    Connection = TConnectionPool::Get()->Connect(HostName, socketTimeout);

    LOG_DEBUG("REQ %s - connection #%u",
        ~RequestId,
        Connection->Id);
}

THttpOutput* THttpRequest::StartRequest(const THttpHeader& header)
{
    auto strHeader = header.GetHeader(HostName, RequestId);
    Url_ = header.GetUrl();
    LOG_DEBUG("REQ %s - %s",
        ~RequestId,
        ~Url_);

    auto parameters = header.GetParameters();
    if (!parameters.Empty()) {
        LOG_DEBUG("REQ %s - X-YT-Parameters: %s",
            ~RequestId,
            ~parameters);
    }

    auto dataStreamFormat = header.GetDataStreamFormat();
    if (dataStreamFormat == DSF_YSON_TEXT) {
        LogResponse = true;
    }

    SocketOutput.Reset(new TSocketOutput(*Connection->Socket.Get()));
    Output.Reset(new THttpOutput(SocketOutput.Get()));
    Output->EnableKeepAlive(true);

    Output->Write(~strHeader, +strHeader);
    return Output.Get();
}

void THttpRequest::FinishRequest()
{
    Output->Flush();
    Output->Finish();
}

THttpResponse* THttpRequest::GetResponseStream()
{
    SocketInput.Reset(new TSocketInput(*Connection->Socket.Get()));
    if (TConfig::Get()->UseAbortableResponse) {
        Y_VERIFY(!Url_.empty());
        Input.Reset(new TAbortableHttpResponse(SocketInput.Get(), RequestId, HostName, Url_));
    } else {
        Input.Reset(new THttpResponse(SocketInput.Get(), RequestId, HostName));
    }
    Input->CheckErrorResponse();
    return Input.Get();
}

Stroka THttpRequest::GetResponse()
{
    Stroka result = GetResponseStream()->ReadAll();
    if (LogResponse) {
        const size_t sizeLimit = 2 << 10;
        if (result.size() > sizeLimit) {
            LOG_DEBUG("RSP %s - %s...truncated - %" PRISZT " bytes total",
                ~RequestId,
                ~result.substr(0, sizeLimit),
                result.size());
        } else {
            LOG_DEBUG("RSP %s - %s",
                ~RequestId,
                ~result);
        }
    } else {
        LOG_INFO("RSP %s - %" PRISZT " bytes",
            ~RequestId,
            result.size());
    }
    return result;
}

void THttpRequest::InvalidateConnection()
{
    TConnectionPool::Get()->Invalidate(HostName, Connection);
    Connection.Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
