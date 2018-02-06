#pragma once

#include "http.h"

#include <util/generic/intrlist.h>

namespace NYT {

class TAbortableHttpResponseRegistry;

using TOutageId = size_t;

class TAbortedForTestPurpose
    : public yexception
{ };

//
// Class extends NYT::THttpResponse with possibility to emultate errors.
class TAbortableHttpResponse
    : public THttpResponse
    , public TIntrusiveListItem<TAbortableHttpResponse>
{
public:
    class TOutage {
    public:
        TOutage(TString urlPattern, size_t responseCount, TAbortableHttpResponseRegistry& registry);
        TOutage(TOutage&&) = default;
        TOutage(const TOutage&) = delete;
        ~TOutage();

        void Stop();

    private:
        TString UrlPattern_;
        TAbortableHttpResponseRegistry& Registry_;
        TOutageId Id_;
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

    // Starts outage. `responseCount` future responses which match `urlPattern` (i.e. contain it in url) will fail.
    // Outage stops when object is destroyed.
    static TOutage StartOutage(const TString& urlPattern, size_t responseCount = std::numeric_limits<size_t>::max());

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
