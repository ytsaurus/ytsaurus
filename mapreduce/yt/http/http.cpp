#include "http.h"

#include "error.h"

#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/config.h>

#include <library/json/json_writer.h>

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

void THttpHeader::SetChunkedEncoding()
{
    ChunkedEncoding = true;
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

    if (ChunkedEncoding) {
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

    if (InputFormat) {
        header << "X-YT-Input-Format: " << InputFormat << "\r\n";
    }

    if (OutputFormat) {
        header << "X-YT-Output-Format: " << OutputFormat << "\r\n";
    }

    if (Parameters) {
        header << "X-YT-Parameters: " << Parameters << "\r\n";
    }

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

TYtHttpResponse::TYtHttpResponse(
    TInputStream* socketStream,
    const Stroka& requestId,
    const Stroka& proxyHostName)
    : HttpInput_(socketStream)
    , RequestId_(requestId)
    , ProxyHostName_(proxyHostName)
{
    HttpCode_ = ParseHttpRetCode(HttpInput_.FirstLine());
    if (HttpCode_ == 200 || HttpCode_ == 202) {
        return;
    }

    TErrorResponse errorResponse(HttpCode_, RequestId_);

    auto logAndSetError = [&] (const Stroka& rawError) {
        LOG_ERROR(
            "RSP %s - HTTP %d - %s",
            ~RequestId_, HttpCode_, ~rawError);
        errorResponse.SetRawError(rawError);
    };

    switch (HttpCode_) {
        case 401:
            logAndSetError("authentication error");
            break;

        case 429:
            logAndSetError("request rate limit exceeded");
            break;

        case 500:
            logAndSetError(TStringBuilder() << "internal error in proxy " << ProxyHostName_);
            break;

        case 503:
            logAndSetError(TStringBuilder() << "proxy " << ProxyHostName_ << " is unavailable");
            break;

        default: {
            TStringStream httpHeaders;;
            httpHeaders << "HTTP headers (";
            for (const auto& header : HttpInput_.Headers()) {
                httpHeaders << header.Name() << ": " << header.Value() << "; ";
            }
            httpHeaders << ")";
            LOG_ERROR(
                "RSP %s - HTTP %d - %s",
                ~RequestId_, HttpCode_, ~httpHeaders.Str());

            if (auto parsedResponse = ParseError(HttpInput_.Headers())) {
                errorResponse = parsedResponse.GetRef();
            } else {
                errorResponse.SetRawError("X-YT-Error is missing in headers");
            }
            break;
        }
    }

    ythrow errorResponse;
}

TMaybe<TErrorResponse> TYtHttpResponse::ParseError(const THttpHeaders& headers)
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

size_t TYtHttpResponse::DoRead(void* buf, size_t len)
{
    size_t read = HttpInput_.Read(buf, len);
    if (read == 0 && len != 0) {
        CheckTrailers(HttpInput_.Trailers().GetRef());
    }
    return read;
}

size_t TYtHttpResponse::DoSkip(size_t len)
{
    size_t skipped = HttpInput_.Skip(len);
    if (skipped == 0 && len != 0) {
        CheckTrailers(HttpInput_.Trailers().GetRef());
    }
    return skipped;
}

void TYtHttpResponse::CheckTrailers(const THttpHeaders& trailers)
{
    if (auto errorResponse = ParseError(trailers)) {
        LOG_ERROR("%s", errorResponse.GetRef().what());
        ythrow errorResponse.GetRef();
    }
}

////////////////////////////////////////////////////////////////////////////////

THttpRequest::THttpRequest(const Stroka& hostName)
    : HostName(hostName)
{
    RequestId = CreateGuidAsString();
}

Stroka THttpRequest::GetRequestId() const
{
    return RequestId;
}

void THttpRequest::Connect(TDuration socketTimeout)
{
    LOG_DEBUG("REQ %s - connect to %s", ~RequestId, ~HostName);

    NetworkAddress = TAddressCache::Get()->Resolve(HostName);

    TSocketHolder socket(DoConnect());
    SetNonBlock(socket, false);

    if (socketTimeout == TDuration::Zero()) {
        socketTimeout = TConfig::Get()->SocketTimeout;
    }
    SetSocketTimeout(socket, socketTimeout.Seconds());

    Socket.Reset(new TSocket(socket.Release()));

    LOG_DEBUG("REQ %s - connected", ~RequestId);
}

SOCKET THttpRequest::DoConnect()
{
    int lastError = 0;

    for (auto i = NetworkAddress->Begin(); i != NetworkAddress->End(); ++i) {
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

    ythrow TSystemError(lastError) << "can not connect to " << *NetworkAddress;
}

THttpOutput* THttpRequest::StartRequest(const THttpHeader& header)
{
    auto strHeader = header.GetHeader(HostName, RequestId);
    LOG_DEBUG("REQ %s - %s", ~RequestId, ~header.GetUrl());

    auto parameters = header.GetParameters();
    if (!parameters.Empty()) {
        LOG_DEBUG("REQ %s - X-YT-Parameters: %s", ~RequestId, ~parameters);
    }

    auto dataStreamFormat = header.GetDataStreamFormat();
    if (dataStreamFormat == DSF_YSON_TEXT) {
        LogResponse = true;
    }

    SocketOutput.Reset(new TSocketOutput(*Socket.Get()));
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

TYtHttpResponse* THttpRequest::GetResponseStream()
{
    SocketInput.Reset(new TSocketInput(*Socket.Get()));
    Input.Reset(new TYtHttpResponse(SocketInput.Get(), RequestId, HostName));
    return Input.Get();
}

Stroka THttpRequest::GetResponse()
{
    Stroka result = GetResponseStream()->ReadAll();
    if (LogResponse) {
        const size_t sizeLimit = 2 << 10;
        LOG_DEBUG("RSP %s - %s", ~RequestId, ~result.substr(0, sizeLimit));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
