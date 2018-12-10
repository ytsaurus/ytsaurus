#pragma once

#include "http.h"

#include <yt/core/net/public.h>
#include <yt/core/net/connection.h>
#include <yt/core/net/address.h>

#include <yt/contrib/http-parser/http_parser.h>

#include <util/stream/buffer.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMessageType,
    (Request)
    (Response)
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EParserState,
    (Initialized)
    (HeadersFinished)
    (MessageFinished)
);

class THttpParser
{
public:
    explicit THttpParser(http_parser_type parserType);

    static http_parser_settings GetParserSettings();

    std::pair<int, int> GetVersion() const;
    EMethod GetMethod() const;
    EStatusCode GetStatusCode() const;
    TString GetFirstLine();

    const THeadersPtr& GetHeaders() const;
    const THeadersPtr& GetTrailers() const;

    void Reset();
    bool ShouldKeepAlive() const;

    EParserState GetState() const;
    TSharedRef GetLastBodyChunk();
    TSharedRef Feed(const TSharedRef& buf);

private:
    http_parser Parser_;

    TStringBuilder FirstLine_;
    TStringBuilder NextField_;
    TStringBuilder NextValue_;

    THeadersPtr Headers_;
    THeadersPtr Trailers_;

    EParserState State_ = EParserState::Initialized;

    const TSharedRef* InputBuffer_ = nullptr;
    TSharedRef LastBodyChunk_;

    bool ShouldKeepAlive_ = false;

    void MaybeFlushHeader(bool trailer);

    static int OnUrl(http_parser* parser, const char *at, size_t length);
    static int OnStatus(http_parser* parser, const char *at, size_t length);
    static int OnHeaderField(http_parser* parser, const char *at, size_t length);
    static int OnHeaderValue(http_parser* parser, const char *at, size_t length);
    static int OnHeadersComplete(http_parser* parser);
    static int OnBody(http_parser* parser, const char *at, size_t length);
    static int OnMessageComplete(http_parser* parser);
};

////////////////////////////////////////////////////////////////////////////////

class THttpInput
    : public IRequest
    , public IResponse
{
public:
    THttpInput(
        const NNet::IConnectionPtr& connection,
        const NNet::TNetworkAddress& peerAddress,
        const IInvokerPtr& readInvoker,
        EMessageType messageType,
        const THttpIOConfigPtr& config);

    virtual EMethod GetMethod() override;
    virtual const TUrlRef& GetUrl() override;
    virtual std::pair<int, int> GetVersion() override;
    virtual const THeadersPtr& GetHeaders() override;

    virtual EStatusCode GetStatusCode() override;
    virtual const THeadersPtr& GetTrailers() override;

    virtual TFuture<TSharedRef> Read() override;

    virtual const NNet::TNetworkAddress& GetRemoteAddress() const override;

    virtual TGuid GetConnectionId() const override;
    void SetConnectionId(TGuid connectionId);

    virtual TGuid GetRequestId() const override;
    void SetRequestId(TGuid requestId);

    virtual i64 GetReadByteCount() const override;

    bool IsExpecting100Continue() const;

    bool IsSafeToReuse() const;
    void Reset();

    // Returns false if connection was closed before receiving first byte.
    bool ReceiveHeaders();

private:
    const NNet::IConnectionPtr Connection_;
    const NNet::TNetworkAddress RemoteAddress_;
    const EMessageType MessageType_;
    const THttpIOConfigPtr Config_;

    TSharedMutableRef InputBuffer_;
    TSharedRef UnconsumedData_;

    bool HeadersReceived_ = false;
    THttpParser Parser_;

    TString RawUrl_;
    TUrlRef Url_;
    THeadersPtr Headers_;

    // Debug.
    TGuid ConnectionId_;
    TGuid RequestId_;
    i64 StartByteCount_ = 0;
    NNet::TConnectionStatistics StartStatistics_;
    TInstant LastProgressLogTime_;

    bool SafeToReuse_ = false;

    void FinishHeaders();
    void FinishMessage();
    void EnsureHeadersReceived();

    IInvokerPtr ReadInvoker_;

    TSharedRef DoRead();

    void MaybeLogSlowProgress();
};

DEFINE_REFCOUNTED_TYPE(THttpInput)

////////////////////////////////////////////////////////////////////////////////

class THttpOutput
    : public IResponseWriter
{
public:
    THttpOutput(
        const THeadersPtr& headers,
        const NNet::IConnectionPtr& connection,
        EMessageType messageType,
        const THttpIOConfigPtr& config);

    THttpOutput(
        const NNet::IConnectionPtr& connection,
        EMessageType messageType,
        const THttpIOConfigPtr& config);

    virtual const THeadersPtr& GetHeaders() override;
    void SetHeaders(const THeadersPtr& headers);
    void SetHost(TStringBuf host, TStringBuf port);
    virtual bool IsHeadersFlushed() const override;

    virtual const THeadersPtr& GetTrailers() override;

    void Flush100Continue();

    void WriteRequest(EMethod method, const TString& path);
    std::optional<EStatusCode> GetStatus() const override;
    virtual void SetStatus(EStatusCode status) override;

    virtual TFuture<void> Write(const TSharedRef& data) override;
    virtual TFuture<void> Close() override;

    virtual TFuture<void> WriteBody(const TSharedRef& smallBody) override;

    virtual void AddConnectionCloseHeader() override;

    bool IsSafeToReuse() const;
    void Reset();

    void SetConnectionId(TGuid connectionId);
    void SetRequestId(TGuid requestId);

    virtual i64 GetWriteByteCount() const override;

private:
    const NNet::IConnectionPtr Connection_;
    const EMessageType MessageType_;
    const THttpIOConfigPtr Config_;

    TClosure OnWriteFinish_;

    //! Debugging.
    TGuid ConnectionId_;
    TGuid RequestId_;
    i64 StartByteCount_ = 0;
    NNet::TConnectionStatistics StartStatistics_;
    bool HeadersLogged_ = false;
    TInstant LastProgressLogTime_;

    static const THashSet<TString> FilteredHeaders_;

    bool ConnectionClose_ = false;

    //! Headers.
    THeadersPtr Headers_;
    std::optional<EStatusCode> Status_;
    std::optional<EMethod> Method_;
    std::optional<TString> HostHeader_;
    TString Path_;
    bool HeadersFlushed_ = false;
    bool MessageFinished_ = false;

    //! Trailers.
    THeadersPtr Trailers_;

    TFuture<void> FinishChunked();

    TSharedRef GetHeadersPart(std::optional<size_t> contentLength);
    TSharedRef GetTrailersPart();

    static TSharedRef GetChunkHeader(size_t size);

    static const TSharedRef Http100Continue;
    static const TSharedRef CrLf;
    static const TSharedRef ZeroCrLf;
    static const TSharedRef ZeroCrLfCrLf;

    void OnWriteFinish();
};

DEFINE_REFCOUNTED_TYPE(THttpOutput)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
