#include "companion_profiling_proxy.h"

#include "private.h"

#include <yt/yt/core/concurrency/poller.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/config.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/server.h>

#include <yt/yt/core/misc/finally.h>

#include <library/cpp/cgiparam/cgiparam.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NHttp;
using namespace NHttp::NHeaders;

constinit const auto Logger = NodeLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

//! The worker-side prefix stripped to get the companion-side path.
constexpr TStringBuf CompanionPrefix = "/companion";
//! The only companion subtree the proxy exposes.
constexpr TStringBuf ProfilingPrefix = "/ytprof/";
//! The companion executable: far larger than any profile and not worth buffering in the worker.
constexpr TStringBuf BinaryPath = "/ytprof/binary";

// ytprof replies only after the profile is collected for its |d| duration and symbolized,
// which takes longer than the default header timeout allows.
constexpr auto ProfileHeaderReadTimeout = TDuration::Minutes(10);
//! Leaves the rest of the header timeout for symbolization.
constexpr auto MaxProfileDuration = TDuration::Minutes(5);

void ReplyWithMessage(const IResponseWriterPtr& rsp, EStatusCode status, const std::string& message)
{
    rsp->SetStatus(status);
    rsp->GetHeaders()->Set(ContentTypeHeaderName, "text/plain; charset=utf-8");
    WaitFor(rsp->WriteBody(TSharedRef::FromString(message)))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

class TCompanionProfilingProxyHandler
    : public IHttpHandler
{
public:
    TCompanionProfilingProxyHandler(IClientPtr client, int companionMonitoringPort)
        : Client_(std::move(client))
        , CompanionMonitoringPort_(companionMonitoringPort)
    { }

    void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        if (req->GetMethod() != EMethod::Get) {
            ReplyWithMessage(rsp, EStatusCode::MethodNotAllowed, "Only GET requests are supported");
            return;
        }

        const auto& url = req->GetUrl();
        // The handler is registered under |CompanionPrefix|, so the path starts with it.
        auto path = url.Path.substr(CompanionPrefix.size());

        if (path == BinaryPath) {
            ReplyWithMessage(
                rsp,
                EStatusCode::NotFound,
                "The companion binary is not served through the worker; fetch it from the companion monitoring port directly");
            return;
        }

        TCgiParameters params(url.RawQuery);
        if (auto it = params.Find("d"); it != params.end()) {
            // A malformed duration is left for the companion to reject.
            TDuration duration;
            if (TDuration::TryParse(it->second, duration) && duration > MaxProfileDuration) {
                ReplyWithMessage(
                    rsp,
                    EStatusCode::BadRequest,
                    Format("Profile duration %v exceeds the limit of %v minutes", it->second, MaxProfileDuration.Minutes()));
                return;
            }
        }

        auto companionUrl = Format("http://localhost:%v%v", CompanionMonitoringPort_, path);
        if (!url.RawQuery.empty()) {
            companionUrl += Format("?%v", url.RawQuery);
        }

        // Held until the reply is written: ytprof moves on to the next profile while a slow
        // client still keeps this one in the worker's memory.
        auto requestsInFlight = ++RequestsInFlight_;
        auto releaseSlot = Finally([this] {
            --RequestsInFlight_;
        });
        if (requestsInFlight > MaxCompanionProfilingRequestsInFlight) {
            ReplyWithMessage(
                rsp,
                EStatusCode::TooManyRequests,
                Format("At most %v companion profiling requests are served at once", MaxCompanionProfilingRequestsInFlight));
            return;
        }

        IResponsePtr companionRsp;
        TSharedRef body;
        try {
            companionRsp = WaitFor(Client_->Get(companionUrl))
                .ValueOrThrow();
            // Buffered rather than streamed: a response already started cannot be aborted,
            // so a companion failure in the middle of the body would pass for a complete reply.
            body = companionRsp->ReadAll();
        } catch (const std::exception& ex) {
            YT_TLOG_WARNING("Failed to fetch a profile from the companion process")
                .With("Url", companionUrl)
                .With(ex);
            ReplyWithMessage(
                rsp,
                EStatusCode::BadGateway,
                Format("Failed to fetch %v from the companion process: %v", path, ex.what()));
            return;
        }

        // Only the status is relayed: the companion headers, e.g. its request id, describe its own reply.
        rsp->SetStatus(companionRsp->GetStatusCode());
        WaitFor(rsp->WriteBody(body))
            .ThrowOnError();
    }

private:
    const IClientPtr Client_;
    const int CompanionMonitoringPort_;

    std::atomic<int> RequestsInFlight_ = 0;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void RegisterCompanionProfilingProxy(
    const IServerPtr& server,
    const TClientConfigPtr& clientConfig,
    const IPollerPtr& poller,
    int companionMonitoringPort)
{
    auto profilingClientConfig = CloneYsonStruct(clientConfig);
    profilingClientConfig->HeaderReadTimeout = std::max(
        profilingClientConfig->HeaderReadTimeout,
        ProfileHeaderReadTimeout);

    server->AddHandler(
        Format("%v%v", CompanionPrefix, ProfilingPrefix),
        New<TCompanionProfilingProxyHandler>(
            CreateClient(std::move(profilingClientConfig), poller),
        companionMonitoringPort));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
