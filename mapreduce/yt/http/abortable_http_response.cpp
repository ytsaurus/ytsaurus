#include "abortable_http_response.h"

#include <util/system/mutex.h>
#include <util/generic/singleton.h>
#include <util/generic/hash_set.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TAbortableHttpResponseRegistry {
public:
    void StartOutage(TString urlPattern)
    {
        auto g = Guard(Lock_);
        BrokenUrlPatterns_.insert(std::move(urlPattern));
    }

    void StopOutage(const TString& urlPattern)
    {
        auto g = Guard(Lock_);
        BrokenUrlPatterns_.erase(urlPattern);
    }

    void Add(TAbortableHttpResponse* response)
    {
        auto g = Guard(Lock_);
        for (const auto& pattern : BrokenUrlPatterns_) {
            if (response->GetUrl().find(pattern) != TString::npos) {
                response->Abort();
            }
        }
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
    THashMultiSet<TString> BrokenUrlPatterns_;
    TMutex Lock_;
};

////////////////////////////////////////////////////////////////////////////////

TAbortableHttpResponse::TOutage::TOutage(TString urlPattern, TAbortableHttpResponseRegistry& registry)
    : UrlPattern_(std::move(urlPattern))
    , Registry_(registry)
{
    Registry_.StartOutage(UrlPattern_);
}

TAbortableHttpResponse::TOutage::~TOutage()
{
    Stop();
}

void TAbortableHttpResponse::TOutage::Stop()
{
    if (!Stopped_) {
        Registry_.StopOutage(UrlPattern_);
        Stopped_ = true;
    }
}

////////////////////////////////////////////////////////////////////////////////

TAbortableHttpResponse::TAbortableHttpResponse(
    IInputStream* socketStream,
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

TAbortableHttpResponse::TOutage TAbortableHttpResponse::StartOutage(const TString& urlPattern)
{
    return TOutage(urlPattern, TAbortableHttpResponseRegistry::Get());
}

const TString& TAbortableHttpResponse::GetUrl() const
{
    return Url_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
