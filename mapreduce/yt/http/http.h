#pragma once

#include "error.h"

#include <mapreduce/yt/interface/common.h>
#include <mapreduce/yt/interface/node.h>
#include <mapreduce/yt/interface/io.h>

#include <library/http/io/stream.h>

#include <util/generic/strbuf.h>
#include <util/generic/guid.h>
#include <util/network/socket.h>
#include <util/stream/input.h>
#include <util/system/mutex.h>
#include <util/system/rwlock.h>
#include <util/generic/ptr.h>

namespace NYT {

class TNode;

///////////////////////////////////////////////////////////////////////////////

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
    bool HasMutationId() const;

    void SetToken(const Stroka& token);

    void SetDataStreamFormat(EDataStreamFormat format);
    EDataStreamFormat GetDataStreamFormat() const;

    void SetInputFormat(const Stroka& format);
    void SetOutputFormat(const Stroka& format);

    void SetParameters(const Stroka& parameters);
    void SetParameters(const TNode& parameters);
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

    yhash<Stroka, Stroka> Params;

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
    yhash<Stroka, TAddressPtr> Cache_;
    TRWMutex Lock_;
};

////////////////////////////////////////////////////////////////////////////////

struct TConnection
{
    THolder<TSocket> Socket;
    TAtomic Busy = 1;
    TInstant DeadLine;
    ui32 Id;
};

using TConnectionPtr = TAtomicSharedPtr<TConnection>;

class TConnectionPool
{
public:
    using TConnectionMap = yhash_mm<Stroka, TConnectionPtr>;

    static TConnectionPool* Get();

    TConnectionPtr Connect(const Stroka& hostName, TDuration socketTimeout);
    void Release(TConnectionPtr connection);
    void Invalidate(const Stroka& hostName, TConnectionPtr connection);

private:
    void Refresh();
    static SOCKET DoConnect(TAddressCache::TAddressPtr address);

private:
    TConnectionMap Connections_;
    TMutex Lock_;
};

////////////////////////////////////////////////////////////////////////////////

//
// Input stream that handles YT-specific header/trailer errors
// and throws TErrorResponse if it finds any.
class THttpResponse
    : public TInputStream
{
public:
    // 'requestId' and 'hostName' are provided for debug reasons
    // (they will appear in some error messages).
    THttpResponse(
        TInputStream* socketStream,
        const Stroka& requestId,
        const Stroka& hostName);

    const THttpHeaders& Headers() const;

    void CheckErrorResponse() const;
    bool IsExhausted() const;

protected:
    size_t DoRead(void* buf, size_t len) override;
    size_t DoSkip(size_t len) override;

private:
    void CheckTrailers(const THttpHeaders& trailers);
    TMaybe<TErrorResponse> ParseError(const THttpHeaders& headers);

private:
    THttpInput HttpInput_;
    const Stroka RequestId_;
    const Stroka HostName_;
    int HttpCode_ = 0;
    TMaybe<TErrorResponse> ErrorResponse_;
    bool IsExhausted_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class THttpRequest
{
public:
    explicit THttpRequest(const Stroka& hostName);
    ~THttpRequest();

    Stroka GetRequestId() const;

    void Connect(TDuration socketTimeout = TDuration::Zero());
    THttpOutput* StartRequest(const THttpHeader& request);
    void FinishRequest();
    THttpResponse* GetResponseStream();

    Stroka GetResponse();

    void InvalidateConnection();

private:
    Stroka HostName;
    Stroka RequestId;
    Stroka Url_;

    TConnectionPtr Connection;

    THolder<TSocketOutput> SocketOutput;
    THolder<THttpOutput> Output;

    THolder<TSocketInput> SocketInput;
    THolder<THttpResponse> Input;

    bool LogResponse = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
