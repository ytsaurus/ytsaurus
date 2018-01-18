#pragma once

#include "http.h"

#include <yt/core/net/public.h>

#include <yt/contrib/http-parser/http_parser.h>

#include <util/stream/buffer.h>


namespace NYT {
namespace NHttp {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMessageType,
    (Request)
    (Response)
);

////////////////////////////////////////////////////////////////////////////////

//! YT enum doesn't support specifying custom string conversion, so we
//! define our own.

TStringBuf ToHttpString(EMethod method);
TStringBuf ToHttpString(EStatusCode code);

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
        const NConcurrency::IAsyncInputStreamPtr& reader,
        const IInvokerPtr& readInvoker,
        EMessageType messageType,
        size_t bufferSize);

    virtual EMethod GetMethod() override;
    virtual const TUrlRef& GetUrl() override;
    virtual std::pair<int, int> GetVersion() override;
    virtual const THeadersPtr& GetHeaders() override;

    virtual EStatusCode GetStatusCode() override;
    virtual const THeadersPtr& GetTrailers() override;

    virtual TFuture<TSharedRef> Read() override;
    virtual TSharedRef ReadBody() override;
    
private:
    const NConcurrency::IAsyncInputStreamPtr Reader_;
    const EMessageType MessageType_;

    TSharedMutableRef InputBuffer_;
    TSharedRef UnconsumedData_;

    bool HeadersReceived_ = false;
    THttpParser Parser_;

    TString RawUrl_;
    TUrlRef Url_;
    THeadersPtr Headers_;

    void FinishHeaders();
    void EnsureHeadersReceived();

    IInvokerPtr ReadInvoker_;

    TSharedRef DoRead();
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
        size_t bufferSize);

    THttpOutput(
        const NNet::IConnectionPtr& connection,
        EMessageType messageType,
        size_t bufferSize);

    virtual const THeadersPtr& GetHeaders() override;
    void SetHeaders(const THeadersPtr& headers);
    void SetHost(TStringBuf host, TStringBuf port);
    bool IsHeadersFlushed() const;

    virtual const THeadersPtr& GetTrailers() override;

    void WriteRequest(EMethod method, const TString& path);
    virtual void WriteHeaders(EStatusCode status) override;

    virtual TFuture<void> Write(const TSharedRef& data) override;
    virtual TFuture<void> Close() override;

    virtual TFuture<void> WriteBody(const TSharedRef& smallBody) override;

    void SendConnectionCloseHeader();

private:
    const NNet::IConnectionPtr Connection_;
    const EMessageType MessageType_;

    static const yhash_set<TString> FilteredHeaders_;

    bool ConnectionClose_ = false;

    //! Headers.
    THeadersPtr Headers_;
    TNullable<EStatusCode> Status_;
    TNullable<EMethod> Method_;
    TNullable<TString> HostHeader_;
    TString Path_;
    bool HeadersFlushed_ = false;
    bool MessageFinished_ = false;

    //! Trailers.
    THeadersPtr Trailers_;

    TFuture<void> FinishChunked();

    TSharedRef GetHeadersPart(TNullable<size_t> contentLength);
    TSharedRef GetTrailersPart();

    static TSharedRef GetChunkHeader(size_t size);

    static const TSharedRef CrLf;
    static const TSharedRef ZeroCrLf;
    static const TSharedRef ZeroCrLfCrLf;
};

DEFINE_REFCOUNTED_TYPE(THttpOutput)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NYT
