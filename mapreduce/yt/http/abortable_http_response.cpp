#include "abortable_http_response.h"

#include <util/system/mutex.h>
#include <util/generic/singleton.h>

namespace NYT {

class TAbortableHttpResponseRegistry {
public:
    void Add(TAbortableHttpResponse* response)
    {
        auto g = Guard(Lock_);
        ResponseList_.PushBack(response);
    }

    void Remove(TAbortableHttpResponse* response)
    {
        auto g = Guard(Lock_);
        response->Unlink();
    }

    static TAbortableHttpResponseRegistry& Get()
    {
        return *Singleton<TAbortableHttpResponseRegistry>();
    }

    int AbortAll(const TString& urlPattern)
    {
        int result = 0;
        for (auto& response : ResponseList_) {
            if (response.GetUrl().find(urlPattern) != TString::npos) {
                response.Abort();
                ++result;
            }
        }
        return result;
    }

private:
    TIntrusiveList<TAbortableHttpResponse> ResponseList_;
    TMutex Lock_;
};

TAbortableHttpResponse::TAbortableHttpResponse(
    TInputStream* socketStream,
    const TString& requestId,
    const TString& hostName,
    const TString& url)
    : THttpResponse(socketStream, requestId, hostName)
    , Url_(url)
{
    TAbortableHttpResponseRegistry::Get().Add(this);
}

TAbortableHttpResponse::~TAbortableHttpResponse()
{
    TAbortableHttpResponseRegistry::Get().Remove(this);
}

size_t TAbortableHttpResponse::DoRead(void* buf, size_t len)
{
    if (Aborted_) {
        ythrow yexception() << "Response stream was aborted";
    }
    return THttpResponse::DoRead(buf, len);
}

size_t TAbortableHttpResponse::DoSkip(size_t len)
{
    if (Aborted_) {
        ythrow yexception() << "Response stream was aborted";
    }
    return THttpResponse::DoSkip(len);
}

void TAbortableHttpResponse::Abort()
{
    Aborted_ = true;
}

int TAbortableHttpResponse::AbortAll(const TString& urlPattern)
{
    return TAbortableHttpResponseRegistry::Get().AbortAll(urlPattern);
}

const TString& TAbortableHttpResponse::GetUrl() const
{
    return Url_;
}

} // namespace NYT
