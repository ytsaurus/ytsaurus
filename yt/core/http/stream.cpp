#include "stream.h"

#include <yt/core/net/connection.h>

#include <yt/core/misc/finally.h>

#include <util/generic/buffer.h>

namespace NYT {
namespace NHttp {

using namespace NConcurrency;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

TStringBuf ToHttpString(EMethod method)
{
    switch(method) {
#define XX(num, name, string) case EMethod::name: return STRINGBUF(#string);
    YT_HTTP_METHOD_MAP(XX)
#undef XX
    default: THROW_ERROR_EXCEPTION("Invalid method %v", method);
    }
}

TStringBuf ToHttpString(EStatusCode code)
{
    switch(code) {
#define XX(num, name, string) case EStatusCode::name: return STRINGBUF(#string);
    YT_HTTP_STATUS_MAP(XX)
#undef XX
    default: THROW_ERROR_EXCEPTION("Invalid status code %d", code);
    }
}


////////////////////////////////////////////////////////////////////////////////

http_parser_settings THttpParser::GetParserSettings()
{
    http_parser_settings settings;
    yt_http_parser_settings_init(&settings);

    settings.on_url = &OnUrl;
    settings.on_status = &OnStatus;
    settings.on_header_field = &OnHeaderField;
    settings.on_header_value = &OnHeaderValue;
    settings.on_headers_complete = &OnHeadersComplete;
    settings.on_body = &OnBody;
    settings.on_message_complete = &OnMessageComplete;

    return settings;
}

const http_parser_settings ParserSettings = THttpParser::GetParserSettings();

THttpParser::THttpParser(http_parser_type parserType)
    : Headers_(New<THeaders>())
{
    yt_http_parser_init(&Parser_, parserType);
    Parser_.data = reinterpret_cast<void*>(this);
}

EParserState THttpParser::GetState() const
{
    return State_;
}

TSharedRef THttpParser::Feed(const TSharedRef& input)
{
    InputBuffer_ = &input;
    auto finally = Finally([&] {
        InputBuffer_ = nullptr;
    });

    size_t read = yt_http_parser_execute(&Parser_, &ParserSettings, input.Begin(), input.Size());
    auto http_errno = static_cast<enum http_errno>(Parser_.http_errno);
    if (http_errno != 0 && http_errno != HPE_PAUSED) {
        // 64 bytes before error
        size_t contextStart = read - std::min<size_t>(read, 64);

        // and 64 bytes after error
        size_t contextEnd = std::min(read + 64, input.Size());

        TString errorContext(input.Begin() + contextStart, contextEnd - contextStart);
    
        THROW_ERROR_EXCEPTION("HTTP parse error: %s", yt_http_errno_description(http_errno))
            << TErrorAttribute("parser_error_name", yt_http_errno_name(http_errno))
            << TErrorAttribute("error_context", errorContext);
    }

    if (http_errno == HPE_PAUSED) {
        yt_http_parser_pause(&Parser_, 0);
    }

    return input.Slice(read, input.Size());
}

std::pair<int, int> THttpParser::GetVersion() const
{
    return std::make_pair<int, int>(Parser_.http_major, Parser_.http_minor);
}

EStatusCode THttpParser::GetStatusCode() const
{
    return EStatusCode(Parser_.status_code);
}

EMethod THttpParser::GetMethod() const
{
    return EMethod(Parser_.method);
}

TString THttpParser::GetFirstLine()
{
    return FirstLine_.Flush();
}

const THeadersPtr& THttpParser::GetHeaders() const
{
    return Headers_;
}

const THeadersPtr& THttpParser::GetTrailers() const
{
    return Trailers_;
}

TSharedRef THttpParser::GetLastBodyChunk()
{
    auto chunk = LastBodyChunk_;
    LastBodyChunk_ = EmptySharedRef;
    return chunk;
}

bool THttpParser::ShouldKeepAlive() const
{
    return ShouldKeepAlive_;
}

void THttpParser::MaybeFlushHeader(bool trailer)
{
    if (NextField_.GetLength() == 0) {
        return;
    }

    if (trailer) {
        if (!Trailers_) {
            Trailers_ = New<THeaders>();
        }
        Trailers_->Set(NextField_.Flush(), NextValue_.Flush());
    } else {
        Headers_->Set(NextField_.Flush(), NextValue_.Flush());
    }     
}
    
int THttpParser::OnUrl(http_parser* parser, const char *at, size_t length)
{
    auto that = reinterpret_cast<THttpParser*>(parser->data);
    that->FirstLine_.AppendString(TStringBuf(at, length));

    return 0;
}

int THttpParser::OnStatus(http_parser* parser, const char *at, size_t length)
{
    return 0;
}

int THttpParser::OnHeaderField(http_parser* parser, const char *at, size_t length)
{
    auto that = reinterpret_cast<THttpParser*>(parser->data);
    that->MaybeFlushHeader(that->State_ == EParserState::HeadersFinished);

    that->NextField_.AppendString(TStringBuf(at, length));
    return 0;
}

int THttpParser::OnHeaderValue(http_parser* parser, const char *at, size_t length)
{
    auto that = reinterpret_cast<THttpParser*>(parser->data);
    that->NextValue_.AppendString(TStringBuf(at, length));
    
    return 0;
}

int THttpParser::OnHeadersComplete(http_parser* parser) 
{
    auto that = reinterpret_cast<THttpParser*>(parser->data);
    that->MaybeFlushHeader(that->State_ == EParserState::HeadersFinished);

    that->State_ = EParserState::HeadersFinished;
    yt_http_parser_pause(parser, 1);

    return 0;
}

int THttpParser::OnBody(http_parser* parser, const char *at, size_t length)
{
    auto that = reinterpret_cast<THttpParser*>(parser->data);
    that->LastBodyChunk_ = that->InputBuffer_->Slice(at, at + length);
    yt_http_parser_pause(parser, 1);
    return 0;
}

int THttpParser::OnMessageComplete(http_parser* parser)
{
    auto that = reinterpret_cast<THttpParser*>(parser->data);
    that->MaybeFlushHeader(that->State_ == EParserState::HeadersFinished);

    that->State_ = EParserState::MessageFinished;
    that->ShouldKeepAlive_ = yt_http_should_keep_alive(parser);
    
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

struct THttpParserTag
{ };

THttpInput::THttpInput(
    const IAsyncInputStreamPtr& reader,
    const TNetworkAddress& remoteAddress,
    const IInvokerPtr& readInvoker,
    EMessageType messageType,
    size_t bufferSize)
    : Reader_(reader)
    , RemoteAddress_(remoteAddress)
    , MessageType_(messageType)
    , InputBuffer_(TSharedMutableRef::Allocate<THttpParserTag>(bufferSize))
    , Parser_(messageType == EMessageType::Request ? HTTP_REQUEST : HTTP_RESPONSE)
    , ReadInvoker_(readInvoker)
{ }

std::pair<int, int> THttpInput::GetVersion()
{
    EnsureHeadersReceived();
    return Parser_.GetVersion();
}

EMethod THttpInput::GetMethod()
{
    YCHECK(MessageType_ == EMessageType::Request);

    EnsureHeadersReceived();
    return Parser_.GetMethod();
}

const TUrlRef& THttpInput::GetUrl()
{
    YCHECK(MessageType_ == EMessageType::Request);

    EnsureHeadersReceived();
    return Url_;
}

const THeadersPtr& THttpInput::GetHeaders()
{
    EnsureHeadersReceived();
    return Headers_;
}

EStatusCode THttpInput::GetStatusCode()
{
    EnsureHeadersReceived();
    return Parser_.GetStatusCode();
}

const THeadersPtr& THttpInput::GetTrailers()
{
    if (Parser_.GetState() != EParserState::MessageFinished) {
        THROW_ERROR_EXCEPTION("Can't access trailers while body is not fully consumed");
    }

    const auto& trailers = Parser_.GetTrailers();
    if (!trailers) {
        static THeadersPtr emptyTrailers = New<THeaders>();
        return emptyTrailers;
    }
    return trailers;
}

const TNetworkAddress& THttpInput::GetRemoteAddress() const
{
    return RemoteAddress_;
}

void THttpInput::FinishHeaders()
{
    HeadersReceived_ = true;
    Headers_ = Parser_.GetHeaders();

    if (MessageType_ == EMessageType::Request) {
        RawUrl_ = Parser_.GetFirstLine();
        Url_ = ParseUrl(RawUrl_);
    }
}

void THttpInput::EnsureHeadersReceived()
{
    if (HeadersReceived_) {
        return;
    }

    while (true) {
        bool eof = false;
        if (UnconsumedData_.Empty()) {
            auto asyncRead = Reader_->Read(InputBuffer_);
            UnconsumedData_ = InputBuffer_.Slice(0, WaitFor(asyncRead).ValueOrThrow());
            eof = UnconsumedData_.Size() == 0;
        }

        UnconsumedData_ = Parser_.Feed(UnconsumedData_);
        if (Parser_.GetState() == EParserState::HeadersFinished) {
            FinishHeaders();
            return;
        }

        // HTTP parser does not treat EOF at message start as error.
        if (eof) {
            THROW_ERROR_EXCEPTION("Unexpected EOF while parsing http message");
        }
    }
}

TFuture<TSharedRef> THttpInput::Read()
{
    return BIND(&THttpInput::DoRead, MakeStrong(this))
        .AsyncVia(ReadInvoker_)
        .Run();
}

TSharedRef THttpInput::ReadBody()
{
    std::vector<TSharedRef> chunks;

    // TODO(prime@): Add hard limit on body size.
    while (true) {
        auto chunk = WaitFor(Read())
            .ValueOrThrow();

        if (chunk.Empty()) {
            break;
        }

        chunks.emplace_back(TSharedRef::MakeCopy<THttpParserTag>(chunk));
    }

    return MergeRefsToRef<THttpParserTag>(std::move(chunks));
}

TSharedRef THttpInput::DoRead()
{
    if (Parser_.GetState() == EParserState::MessageFinished) {
        return EmptySharedRef;
    }

    while (true) {
        auto chunk = Parser_.GetLastBodyChunk();
        if (!chunk.Empty()) {
            return chunk;
        }

        bool eof = false;
        if (UnconsumedData_.Empty()) {
            auto asyncRead = Reader_->Read(InputBuffer_);
            UnconsumedData_ = InputBuffer_.Slice(0, WaitFor(asyncRead).ValueOrThrow());
            eof = UnconsumedData_.Size() == 0;
        }

        UnconsumedData_ = Parser_.Feed(UnconsumedData_);
        if (Parser_.GetState() == EParserState::MessageFinished) {
            return EmptySharedRef;
        }

        // EOF must be handled by HTTP parser.
        YCHECK(!eof);
    }    
}

////////////////////////////////////////////////////////////////////////////////

THttpOutput::THttpOutput(
    const THeadersPtr& headers,
    const IConnectionPtr& connection,
    EMessageType messageType,
    size_t bufferSize)
    : Connection_(connection)
    , MessageType_(messageType)
    , Headers_(headers)
{ }

THttpOutput::THttpOutput(
    const IConnectionPtr& connection,
    EMessageType messageType,
    size_t bufferSize)
    : THttpOutput(New<THeaders>(), connection, messageType, bufferSize)
{ }

const THeadersPtr& THttpOutput::GetHeaders()
{
    return Headers_;
}

void THttpOutput::SetHost(TStringBuf host, TStringBuf port)
{
    if (!port.empty()) {
        HostHeader_ = Format("%v:%v", host, port);
    } else {
        HostHeader_ = TString(host);
    }
}

void THttpOutput::SetHeaders(const THeadersPtr& headers)
{
    Headers_ = headers;
}

bool THttpOutput::IsHeadersFlushed() const
{
    return HeadersFlushed_;
}

const THeadersPtr& THttpOutput::GetTrailers()
{
    if (!Trailers_) {
        Trailers_ = New<THeaders>();
    }
    return Trailers_;
}

void THttpOutput::SendConnectionCloseHeader()
{
    YCHECK(MessageType_ == EMessageType::Response);
    ConnectionClose_ = true;
}

void THttpOutput::WriteRequest(EMethod method, const TString& path)
{
    YCHECK(MessageType_ == EMessageType::Request);

    Method_ = method;
    Path_ = path;
}

void THttpOutput::WriteHeaders(EStatusCode status)
{
    YCHECK(MessageType_ == EMessageType::Response);

    Status_ = status;
}

TSharedRef THttpOutput::GetHeadersPart(TNullable<size_t> contentLength)
{
    TBufferOutput messageHeaders;
    if (MessageType_ == EMessageType::Request) {
        YCHECK(Method_);

        messageHeaders << ToHttpString(*Method_) << " " << Path_ << " HTTP/1.1\r\n";
    } else {
        if (!Status_) {
            Status_ = EStatusCode::Ok;
        }

        messageHeaders << "HTTP/1.1 " << static_cast<int>(*Status_) << " " << ToHttpString(*Status_) << "\r\n";
    }

    bool methodNeedsContentLength = Method_ && *Method_ != EMethod::Get && *Method_ != EMethod::Head;

    if (contentLength) {
        if (MessageType_ == EMessageType::Response ||
            (MessageType_ == EMessageType::Request && methodNeedsContentLength)) {
            messageHeaders << "Content-Length: " << *contentLength << "\r\n";
        }
    } else {
        messageHeaders << "Transfer-Encoding: chunked\r\n";
    }

    if (ConnectionClose_) {
        messageHeaders << "Connection: close\r\n";
    }

    if (HostHeader_) {
        messageHeaders << "Host: " << *HostHeader_ << "\r\n";
    }

    Headers_->WriteTo(&messageHeaders, &FilteredHeaders_);

    TString headers;
    messageHeaders.Buffer().AsString(headers);
    return TSharedRef::FromString(headers);
}

TSharedRef THttpOutput::GetTrailersPart()
{
    TBufferOutput messageTrailers;

    Trailers_->WriteTo(&messageTrailers, &FilteredHeaders_);

    TString trailers;
    messageTrailers.Buffer().AsString(trailers);
    return TSharedRef::FromString(trailers);
}

TSharedRef THttpOutput::GetChunkHeader(size_t size)
{
    return TSharedRef::FromString(Format("%X\r\n", size));
}

TFuture<void> THttpOutput::Write(const TSharedRef& data)
{
    if (MessageFinished_) {
        THROW_ERROR_EXCEPTION("Cannot write to finished HTTP message");
    }

    std::vector<TSharedRef> writeRefs;
    if (!HeadersFlushed_) {
        HeadersFlushed_ = true;
        writeRefs.emplace_back(GetHeadersPart(Null));
        writeRefs.emplace_back(CrLf);
    }

    if (data.Size() != 0) {
        writeRefs.emplace_back(GetChunkHeader(data.Size()));
        writeRefs.emplace_back(data);
        writeRefs.push_back(CrLf);
    }

    return Connection_->WriteV(TSharedRefArray(std::move(writeRefs)));
}

TFuture<void> THttpOutput::Close()
{
    if (MessageFinished_) {
        return VoidFuture;
    }

    if (!HeadersFlushed_) {
        return WriteBody(EmptySharedRef);
    }

    return FinishChunked();
}

TFuture<void> THttpOutput::FinishChunked()
{
    std::vector<TSharedRef> writeRefs;
    
    if (Trailers_) {
        writeRefs.emplace_back(ZeroCrLf);
        writeRefs.emplace_back(GetTrailersPart());
        writeRefs.emplace_back(CrLf);
    } else {
        writeRefs.emplace_back(ZeroCrLfCrLf);
    }

    MessageFinished_ = true;
    return Connection_->WriteV(TSharedRefArray(std::move(writeRefs)));
}

TFuture<void> THttpOutput::WriteBody(const TSharedRef& smallBody)
{
    YCHECK(!HeadersFlushed_ && !MessageFinished_);

    TSharedRefArray writeRefs;
    if (Trailers_) {        
        writeRefs = TSharedRefArray({
            GetHeadersPart(smallBody.Size()),
            GetTrailersPart(),
            CrLf,
            smallBody
        });
    } else {
        writeRefs = TSharedRefArray({
            GetHeadersPart(smallBody.Size()),
            CrLf,
            smallBody
        });
    }

    HeadersFlushed_ = true;
    MessageFinished_ = true;
    return Connection_->WriteV(writeRefs);
}

////////////////////////////////////////////////////////////////////////////////

const yhash_set<TString> THttpOutput::FilteredHeaders_ = {
    "transfer-encoding",
    "content-length",
    "connection",
    "host"
};

const TSharedRef THttpOutput::CrLf = TSharedRef::FromString("\r\n");
const TSharedRef THttpOutput::ZeroCrLf = TSharedRef::FromString("0\r\n");
const TSharedRef THttpOutput::ZeroCrLfCrLf = TSharedRef::FromString("0\r\n\r\n");

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NYT
