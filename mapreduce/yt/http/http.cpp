#include "http.h"

#include "error.h"

#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/config.h>

#include <library/json/json_writer.h>

#include <util/string/quote.h>
#include <util/string/printf.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

THttpHeader::THttpHeader(const Stroka& method, const Stroka& command, bool isApi)
    : Method(method)
    , Command(command)
    , IsApi(isApi)
{ }

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

void THttpHeader::SetFormat(const Stroka& format)
{
    Format = format;
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
    header << "User-Agent: " << TProcessProperties::Get()->ClientVersion << "\r\n";

    Stroka Token = TConfig::Get()->Token;
    if (!Token.Empty()) {
        header << "Authorization: OAuth " << Token << "\r\n";
    }

    if (ChunkedEncoding) {
        header << "Transfer-Encoding: chunked\r\n";
    }

    header << "X-YT-Correlation-Id: " << requestId << "\r\n";

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
    }

    if (!Format.empty()) {
        header << "X-YT-Output-Format: " << Format << "\r\n";
    }

    if (!Parameters.empty()) {
        header << "X-YT-Parameters: " << Parameters << "\r\n";
    }

    header << "\r\n";
    return header.Str();
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

void THttpRequest::Connect()
{
    LOG_DEBUG("REQ %s - connect to %s", ~RequestId, ~HostName);

    NetworkAddress.Reset(new TNetworkAddress(HostName, 80));

    TSocketHolder socket(DoConnect());
    SetNonBlock(socket, false);
    SetSocketTimeout(socket, TConfig::Get()->SendReceiveTimeout.Seconds());
    Socket.Reset(new TSocket(socket.Release()));

    LOG_DEBUG("REQ %s - connected", ~RequestId);
}

SOCKET THttpRequest::DoConnect()
{
    int lastError = 0;

    for (TNetworkAddress::TIterator i = NetworkAddress->Begin(); i != NetworkAddress->End(); ++i) {
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

THttpInput* THttpRequest::GetResponseStream()
{
    SocketInput.Reset(new TSocketInput(*Socket.Get()));
    Input.Reset(new THttpInput(SocketInput.Get()));

    auto httpCode = ParseHttpRetCode(Input->FirstLine());
    if (httpCode == 200 || httpCode == 202) {
        return Input.Get();
    }

    TErrorResponse errorResponse(httpCode, RequestId);

    switch (httpCode) {
        case 401:
            LOG_ERROR(
                "RSP %s - HTTP %d - authentication error",
                ~RequestId, httpCode);
            break;

        case 500:
            LOG_ERROR(
                "RSP %s - HTTP %d - internal error in proxy %s",
                ~RequestId, httpCode, ~HostName);
            break;

        case 503:
            LOG_ERROR(
                "RSP %s - HTTP %d - proxy %s is unavailable",
                ~RequestId, httpCode, ~HostName);
            break;

        default: {
            Stroka ytError;
            Stroka httpHeaders("HTTP headers (");
            for (auto i = Input->Headers().Begin(); i != Input->Headers().End(); ++i) {
                httpHeaders += i->Name();
                httpHeaders += ": ";
                httpHeaders += i->Value();
                httpHeaders += "; ";
                if (i->Name() == "X-YT-Error") {
                    ytError = i->Value();
                    errorResponse.ParseFromJsonError(ytError);
                }
            }
            httpHeaders += ")";
            LOG_ERROR(
                "RSP %s - HTTP %d - %s",
                ~RequestId, httpCode, ~httpHeaders);
            break;
        }
    }

    ythrow errorResponse;
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
