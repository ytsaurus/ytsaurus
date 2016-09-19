#pragma once

#include "error.h"

#include <mapreduce/yt/interface/common.h>
#include <mapreduce/yt/interface/node.h>

#include <library/http/io/stream.h>

#include <util/generic/strbuf.h>
#include <util/generic/guid.h>
#include <util/network/socket.h>
#include <util/stream/input.h>
#include <util/system/rwlock.h>
#include <util/generic/ptr.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

enum EDataStreamFormat
{
    DSF_YSON_TEXT,
    DSF_YSON_BINARY,
    DSF_YAMR_LENVAL,
    DSF_BYTES,
    DSF_PROTO
};

class THttpHeader
{
public:
    THttpHeader(const Stroka& method, const Stroka& command, bool isApi = true);

    void AddParam(const Stroka& key, const char* value);
    void AddParam(const Stroka& key, const Stroka& value);
    void AddParam(const Stroka& key, i64 value);
    void AddParam(const Stroka& key, ui64 value);
    void AddParam(const Stroka& key, bool value);
    void RemoveParam(const Stroka& key);

    void AddTransactionId(const TTransactionId& transactionId);
    void AddPath(const Stroka& path);
    void AddOperationId(const TOperationId& operationId);
    void AddMutationId();

    void SetToken(const Stroka& token);

    void SetDataStreamFormat(EDataStreamFormat format);
    EDataStreamFormat GetDataStreamFormat() const;

    void SetInputFormat(const Stroka& format);
    void SetOutputFormat(const Stroka& format);

    void SetParameters(const Stroka& parameters);
    Stroka GetParameters() const;

    void SetRequestCompression(const Stroka& compression);
    void SetResponseCompression(const Stroka& compression);

    Stroka GetCommand() const;
    Stroka GetUrl() const;
    Stroka GetHeader(const Stroka& hostName, const Stroka& requestId) const;

private:
    const Stroka Method;
    const Stroka Command;
    const bool IsApi;

    yhash_map<Stroka, Stroka> Params;

    Stroka Token;

    TNode Attributes;

    EDataStreamFormat DataStreamFormat = DSF_YSON_TEXT;

    Stroka InputFormat;
    Stroka OutputFormat;
    Stroka Parameters;

    Stroka RequestCompression = "identity";
    Stroka ResponseCompression = "identity";
};

////////////////////////////////////////////////////////////////////////////////

class TAddressCache
{
public:
    using TAddressPtr = TAtomicSharedPtr<TNetworkAddress>;

    static TAddressCache* Get();

    TAddressPtr Resolve(const Stroka& hostName);

private:
    yhash_map<Stroka, TAddressPtr> Cache_;
    TRWMutex Lock_;
};

////////////////////////////////////////////////////////////////////////////////

//
// Input stream that handles YT-specific header/trailer errors
// and throws TErrorResponse if it finds any.
class TYtHttpResponse
    : public TInputStream
{
public:
    // 'requestId' and 'proxyHostName' are provided for debug reasons
    // (they will appear in some error messages).
    TYtHttpResponse(
        TInputStream* socketStream,
        const Stroka& requestId,
        const Stroka& proxyHostName);

private:
    size_t DoRead(void* buf, size_t len) override;
    size_t DoSkip(size_t len) override;

    void CheckTrailers(const THttpHeaders& trailers);
    TMaybe<TErrorResponse> ParseError(const THttpHeaders& headers);

private:
    THttpInput HttpInput_;
    const Stroka RequestId_;
    const Stroka ProxyHostName_;
    int HttpCode_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class THttpRequest
{
public:
    explicit THttpRequest(const Stroka& hostName);

    Stroka GetRequestId() const;

    void Connect(TDuration socketTimeout = TDuration::Zero());
    THttpOutput* StartRequest(const THttpHeader& request);
    void FinishRequest();
    TYtHttpResponse* GetResponseStream();

    Stroka GetResponse();

private:
    SOCKET DoConnect();

private:
    Stroka HostName;
    Stroka RequestId;

    TAtomicSharedPtr<TNetworkAddress> NetworkAddress;
    THolder<TSocket> Socket;

    THolder<TSocketOutput> SocketOutput;
    THolder<THttpOutput> Output;

    THolder<TSocketInput> SocketInput;
    THolder<TYtHttpResponse> Input;

    bool LogResponse = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
