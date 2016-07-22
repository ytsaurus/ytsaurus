#pragma once

#include <mapreduce/yt/interface/common.h>
#include <mapreduce/yt/interface/node.h>

#include <library/http/io/stream.h>

#include <util/generic/strbuf.h>
#include <util/generic/guid.h>
#include <util/network/socket.h>
#include <util/system/rwlock.h>
#include <util/generic/ptr.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

enum EDataStreamFormat
{
    DSF_YSON_TEXT,
    DSF_YSON_BINARY,
    DSF_YAMR_LENVAL,
    DSF_BYTES
// TODO:
//    DSF_PROTO,
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

    void SetChunkedEncoding();

    void SetDataStreamFormat(EDataStreamFormat format);
    EDataStreamFormat GetDataStreamFormat() const;

    void SetFormat(const Stroka& format);

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

    bool ChunkedEncoding = false;

    yhash_map<Stroka, Stroka> Params;

    Stroka Token;

    TNode Attributes;

    EDataStreamFormat DataStreamFormat = DSF_YSON_TEXT;

    Stroka Format;
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

class THttpRequest
{
public:
    explicit THttpRequest(const Stroka& hostName);

    Stroka GetRequestId() const;

    void Connect(TDuration socketTimeout = TDuration::Zero());
    THttpOutput* StartRequest(const THttpHeader& request);
    void FinishRequest();
    THttpInput* GetResponseStream();

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
    THolder<THttpInput> Input;

    bool LogResponse = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
