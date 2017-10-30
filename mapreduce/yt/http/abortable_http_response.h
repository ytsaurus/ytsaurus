#pragma once

#include "http.h"

#include <util/generic/intrlist.h>

namespace NYT {

class TAbortableHttpResponseRegistry;

//
// Class extends NYT::THttpResponse with possibility to emultate errors.
class TAbortableHttpResponse
    : public THttpResponse
    , public TIntrusiveListItem<TAbortableHttpResponse>
{
public:
    class TOutage {
    public:
        TOutage(TString urlPattern, TAbortableHttpResponseRegistry& registry);
        TOutage(TOutage&&) = default;
        TOutage(const TOutage&) = delete;
        ~TOutage();

        void Stop();

    private:
        TString UrlPattern_;
        TAbortableHttpResponseRegistry& Registry_;
        bool Stopped_ = false;
    };

public:
    TAbortableHttpResponse(
        IInputStream* socketStream,
        const TString& requestId,
        const TString& hostName,
        const TString& url);

    ~TAbortableHttpResponse();

    // Aborts any responses which match `urlPattern` (i.e. contain it in url).
    // Returns number of aborted responses.
    static int AbortAll(const TString& urlPattern);

    // Starts outage. All future responses which match `urlPattern` (i.e. contain it in url) will fail.
    // Outage stops when object is destroyed.
    static TOutage StartOutage(const TString& urlPattern);

    void Abort();
    const TString& GetUrl() const;

private:
    size_t DoRead(void* buf, size_t len) override;
    size_t DoSkip(size_t len) override;

private:
    TString Url_;
    std::atomic<bool> Aborted_ = {false};
};

} // namespace NYT
