#include "http_client.h"

#include "http.h"

#include <yt/yt/core/http/http.h>

namespace NYT::NHttpClient {

///////////////////////////////////////////////////////////////////////////////

class TDefaultHttpResponse
    : public IHttpResponse
{
public:
    TDefaultHttpResponse(std::unique_ptr<THttpRequest> request)
        : Request_(std::move(request))
    { }

    int GetStatusCode() override
    {
        return Request_->GetHttpCode();
    }

    IInputStream* GetResponseStream() override
    {
        return Request_->GetResponseStream();
    }

    TString GetResponse() override
    {
        return Request_->GetResponse();
    }

    TString GetRequestId() const override
    {
        return Request_->GetRequestId();
    }

private:
    std::unique_ptr<THttpRequest> Request_;
};

class TDefaultHttpRequest
    : public IHttpRequest
{
public:
    TDefaultHttpRequest(std::unique_ptr<THttpRequest> request, IOutputStream* stream)
        : Request_(std::move(request))
        , Stream_(stream)
    { }

    IOutputStream* GetStream() override
    {
        return Stream_;
    }

    IHttpResponsePtr Finish() override
    {
        Request_->FinishRequest();
        return std::make_unique<TDefaultHttpResponse>(std::move(Request_));
    }

private:
    std::unique_ptr<THttpRequest> Request_;
    IOutputStream* Stream_;
};

class TDefaultHttpClient
    : public IHttpClient
{
public:
    IHttpResponsePtr Request(const TString& url, const TString& requestId, const THttpConfig& config, const THttpHeader& headers, TMaybe<TStringBuf> body) override
    {
        auto request = std::make_unique<THttpRequest>(requestId);

        auto urlRef = NHttp::ParseUrl(url);

        request->Connect(CreateHost(urlRef.Host, urlRef.PortStr), config.SocketTimeout);
        request->SmallRequest(headers, body);
        return std::make_unique<TDefaultHttpResponse>(std::move(request));
    }

    IHttpRequestPtr StartRequest(const TString& url, const TString& requestId, const THttpConfig& config, const THttpHeader& headers) override
    {
        auto request = std::make_unique<THttpRequest>(requestId);

        auto urlRef = NHttp::ParseUrl(url);

        request->Connect(CreateHost(urlRef.Host, urlRef.PortStr), config.SocketTimeout);
        auto stream = request->StartRequest(headers);
        return std::make_unique<TDefaultHttpRequest>(std::move(request), stream);
    }

private:
    TString CreateHost(TStringBuf host, TStringBuf port) const
    {
        if (!port.empty()) {
            return Format("%v:%v", host, port);
        }

        return TString(host);
    }
};

///////////////////////////////////////////////////////////////////////////////

IHttpClientPtr CreateDefaultHttpClient()
{
    return std::make_shared<TDefaultHttpClient>();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpClient
