#include <Server/HTTP/HTTPServerResponse.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <DBPoco/CountingStream.h>
#include <DBPoco/DateTimeFormat.h>
#include <DBPoco/DateTimeFormatter.h>
#include <DBPoco/FileStream.h>
#include <DBPoco/Net/HTTPChunkedStream.h>
#include <DBPoco/Net/HTTPFixedLengthStream.h>
#include <DBPoco/Net/HTTPHeaderStream.h>
#include <DBPoco/Net/HTTPStream.h>
#include <DBPoco/StreamCopier.h>
#include <sstream>


namespace DB
{

HTTPServerResponse::HTTPServerResponse(DBPoco::Net::HTTPServerSession & session_, const ProfileEvents::Event & write_event_)
    : session(session_)
    , write_event(write_event_)
{
}

void HTTPServerResponse::sendContinue()
{
    DBPoco::Net::HTTPHeaderOutputStream hs(session);
    hs << getVersion() << " 100 Continue\r\n\r\n";
}

std::shared_ptr<WriteBufferFromPocoSocket> HTTPServerResponse::send()
{
    DB_poco_assert(!stream);

    if ((request && request->getMethod() == HTTPRequest::HTTP_HEAD) || getStatus() < 200 || getStatus() == HTTPResponse::HTTP_NO_CONTENT
        || getStatus() == HTTPResponse::HTTP_NOT_MODIFIED)
    {
        // Send header
        DBPoco::Net::HTTPHeaderOutputStream hs(session);
        write(hs);
        stream = std::make_shared<WriteBufferFromPocoSocket>(session.socket(), write_event);
    }
    else if (getChunkedTransferEncoding())
    {
        // Send header
        DBPoco::Net::HTTPHeaderOutputStream hs(session);
        write(hs);
        stream = std::make_shared<HTTPWriteBufferChunked>(session.socket(), write_event);
    }
    else if (hasContentLength())
    {
        // Send header
        DBPoco::Net::HTTPHeaderOutputStream hs(session);
        write(hs);
        stream = std::make_shared<HTTPWriteBufferFixedLength>(session.socket(), getContentLength(), write_event);
    }
    else
    {
        setKeepAlive(false);
        // Send header
        DBPoco::Net::HTTPHeaderOutputStream hs(session);
        write(hs);
        stream = std::make_shared<WriteBufferFromPocoSocket>(session.socket(), write_event);
    }

    send_started = true;
    return stream;
}

std::pair<std::shared_ptr<WriteBufferFromPocoSocket>, std::shared_ptr<WriteBufferFromPocoSocket>> HTTPServerResponse::beginSend()
{
    DB_poco_assert(!stream);
    DB_poco_assert(!header_stream);

    /// NOTE: Code is not exception safe.

    if ((request && request->getMethod() == HTTPRequest::HTTP_HEAD) || getStatus() < 200 || getStatus() == HTTPResponse::HTTP_NO_CONTENT
        || getStatus() == HTTPResponse::HTTP_NOT_MODIFIED)
    {
        throw DBPoco::Exception("HTTPServerResponse::beginSend is invalid for HEAD request");
    }

    if (hasContentLength())
    {
        throw DBPoco::Exception("HTTPServerResponse::beginSend is invalid for response with Content-Length header");
    }

    // Write header to buffer
    std::stringstream header; //STYLE_CHECK_ALLOW_STD_STRING_STREAM
    beginWrite(header);
    // Send header
    auto str = header.str();
    header_stream = std::make_shared<WriteBufferFromPocoSocket>(session.socket(), write_event, str.size());
    header_stream->write(str);

    if (getChunkedTransferEncoding())
        stream = std::make_shared<HTTPWriteBufferChunked>(session.socket(), write_event);
    else
        stream = std::make_shared<WriteBufferFromPocoSocket>(session.socket(), write_event);

    send_started = true;
    return std::make_pair(header_stream, stream);
}

void HTTPServerResponse::beginWrite(std::ostream & ostr) const
{
    HTTPResponse::beginWrite(ostr);
    send_started = true;
}

void HTTPServerResponse::sendBuffer(const void * buffer, std::size_t length)
{
    setContentLength(static_cast<int>(length));
    setChunkedTransferEncoding(false);
    // Send header
    DBPoco::Net::HTTPHeaderOutputStream hs(session);
    write(hs);
    hs.flush();

    if (request && request->getMethod() != HTTPRequest::HTTP_HEAD)
        WriteBufferFromPocoSocket(session.socket(), write_event).write(static_cast<const char *>(buffer), length);
}

void HTTPServerResponse::requireAuthentication(const std::string & realm)
{
    DB_poco_assert(!stream);

    setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
    std::string auth("Basic realm=\"");
    auth.append(realm);
    auth.append("\"");
    set("WWW-Authenticate", auth);
}

void HTTPServerResponse::redirect(const std::string & uri, HTTPStatus status)
{
    DB_poco_assert(!stream);

    setContentLength(0);
    setChunkedTransferEncoding(false);

    setStatusAndReason(status);
    set("Location", uri);

    // Send header
    DBPoco::Net::HTTPHeaderOutputStream hs(session);
    write(hs);
    hs.flush();
}

}
