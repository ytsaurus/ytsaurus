#pragma once

#include <mapreduce/yt/interface/common.h>
#include <mapreduce/yt/interface/node.h>

#include <library/http/io/stream.h>

#include <util/generic/strbuf.h>
#include <util/generic/guid.h>
#include <util/network/socket.h>

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

    void AddParam(const Stroka& key, const Stroka& value);
    void AddParam(const Stroka& key, i64 value);
    void AddParam(const Stroka& key, ui64 value);
    void AddParam(const Stroka& key, bool value);
    void RemoveParam(const Stroka& key);

    void AddTransactionId(const TTransactionId& transactionId);
    void AddPath(const Stroka& path);
    void AddOperationId(const TOperationId& operationId);
    void AddMutationId();

    void SetChunkedEncoding();

    void SetDataStreamFormat(EDataStreamFormat format);
    EDataStreamFormat GetDataStreamFormat() const;

    void SetFormat(const Stroka& format);

    void SetParameters(const Stroka& parameters);
    Stroka GetParameters() const;

    void SetRequestCompression(const Stroka& compression);
    void SetResponseCompression(const Stroka& compression);

    Stroka GetUrl() const;
    Stroka GetHeader(const Stroka& hostName, const Stroka& requestId) const;

private:
    Stroka Method;
    Stroka Command;
    bool IsApi;
    bool ChunkedEncoding = false;

    yhash_map<Stroka, Stroka> Params;

    TNode Attributes;

    EDataStreamFormat DataStreamFormat = DSF_YSON_TEXT;

    Stroka Format;
    Stroka Parameters;

    Stroka RequestCompression = "identity";
    Stroka ResponseCompression = "identity";
};

////////////////////////////////////////////////////////////////////////////////

class THttpRequest
{
public:
    explicit THttpRequest(const Stroka& hostName);

    Stroka GetRequestId() const;

    void Connect();
    THttpOutput* StartRequest(const THttpHeader& request);
    void FinishRequest();
    THttpInput* GetResponseStream();

    Stroka GetResponse();

private:
    SOCKET DoConnect();

private:
    Stroka HostName;
    Stroka RequestId;

    THolder<TNetworkAddress> NetworkAddress;
    THolder<TSocket> Socket;

    THolder<TSocketOutput> SocketOutput;
    THolder<THttpOutput> Output;

    THolder<TSocketInput> SocketInput;
    THolder<THttpInput> Input;

    bool LogResponse = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
