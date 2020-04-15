#pragma once

#include "http.h"

#include <util/generic/intrlist.h>

namespace NYT {

class TAbortableHttpResponseRegistry;

using TOutageId = size_t;

class TAbortedForTestPurpose
    : public yexception
{ };

struct TOutageOptions
{
    using TSelf = TOutageOptions;

    /// @brief Number of responses to abort.
    FLUENT_FIELD_DEFAULT(size_t, ResponseCount, std::numeric_limits<size_t>::max())

    /// @brief Number of bytes to read before abortion. If zero, abort immediately.
    FLUENT_FIELD_DEFAULT(size_t, LengthLimit, 0)
};

/// @brief Class extends @ref NYT::THttpResponse with possibility to emulate errors.
class TAbortableHttpResponse
    : public THttpResponse
    , public TIntrusiveListItem<TAbortableHttpResponse>
{
public:
    class TOutage
    {
    public:
        TOutage(TString urlPattern, TAbortableHttpResponseRegistry& registry, const TOutageOptions& options);
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

    /// @brief Abort any responses which match `urlPattern` (i.e. contain it in url).
    ///
    /// @return number of aborted responses.
    static int AbortAll(const TString& urlPattern);

    /// @brief Start outage. Future responses which match `urlPattern` (i.e. contain it in url) will fail.
    ///
    /// @return outage object controlling the lifetime of outage (outage stops when object is destroyed)
    [[nodiscard]] static TOutage StartOutage(
        const TString& urlPattern,
        const TOutageOptions& options = TOutageOptions());

    /// @brief Start outage. Future `responseCount` responses which match `urlPattern` (i.e. contain it in url) will fail.
    ///
    /// @return outage object controlling the lifetime of outage (outage stops when object is destroyed)
    [[nodiscard]] static TOutage StartOutage(
        const TString& urlPattern,
        size_t responseCount);

    void SetLengthLimit(size_t limit);

    void Abort();
    const TString& GetUrl() const;

    bool IsAborted() const;

private:
    size_t DoRead(void* buf, size_t len) override;
    size_t DoSkip(size_t len) override;

private:
    TString Url_;
    std::atomic<bool> Aborted_ = {false};
    size_t LengthLimit_ = std::numeric_limits<size_t>::max();
};

} // namespace NYT
