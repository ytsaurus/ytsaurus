#pragma once

#include "http.h"

#include <util/generic/intrlist.h>

namespace NYT {

//
// Class extends NYT::THttpResponse with possibility to emultate errors.
class TAbortableHttpResponse
    : public THttpResponse
    , public TIntrusiveListItem<TAbortableHttpResponse>
{
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
