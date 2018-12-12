#include "default_tvm_service.h"
#include "tvm_service.h"
#include "config.h"
#include "helpers.h"
#include "private.h"

#include <yt/core/json/json_parser.h>

#include <yt/core/ytree/ypath_client.h>

#include <yt/core/ypath/token.h>

#include <yt/core/http/client.h>
#include <yt/core/http/http.h>

namespace NYT::NAuth {

using namespace NYTree;
using namespace NHttp;
using namespace NYPath;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TDefaultTvmService
    : public ITvmService
{
public:
    TDefaultTvmService(
        TDefaultTvmServiceConfigPtr config,
        IPollerPtr poller,
        NProfiling::TProfiler profiler)
        : Config_(std::move(config))
        , Profiler_(std::move(profiler))
        , HttpClient_(CreateClient(Config_->HttpClient, std::move(poller)))
    { }

    virtual TFuture<TString> GetTicket(const TString& serviceId) override
    {
        YT_LOG_DEBUG("Retrieving TVM ticket (ServiceId: %v)",
            serviceId);

        auto headers = MakeRequestHeaders();

        TSafeUrlBuilder builder;
        builder.AppendString(Format("http://%v:%v/tvm/tickets?", Config_->Host, Config_->Port));
        builder.AppendParam(AsStringBuf("dsts"), serviceId);
        builder.AppendString("&format=json");
        auto safeUrl = builder.FlushSafeUrl();
        auto realUrl = builder.FlushRealUrl();

        auto callId = TGuid::Create();

        YT_LOG_DEBUG("Calling TVM daemon (Url: %v, CallId: %v)",
            safeUrl,
            callId);

        Profiler_.Increment(CallCountCounter_);

        NProfiling::TWallTimer timer;
        return HttpClient_->Get(realUrl, headers)
            .WithTimeout(Config_->RequestTimeout)
            .Apply(BIND(
                &TDefaultTvmService::OnTvmCallResult,
                MakeStrong(this),
                callId,
                serviceId,
                timer));
    }

private:
    const TDefaultTvmServiceConfigPtr Config_;
    const NProfiling::TProfiler Profiler_;

    const IClientPtr HttpClient_;

    NProfiling::TMonotonicCounter CallCountCounter_{"/call_count"};
    NProfiling::TAggregateGauge CallTimeGauge_{"/call_time"};
    NProfiling::TMonotonicCounter SuccessfulCallCountCounter_{"/successful_call_count"};
    NProfiling::TMonotonicCounter FailedCallCountCounter_{"/failed_call_count"};

private:
    THeadersPtr MakeRequestHeaders()
    {
        auto headers = New<THeaders>();
        static const TString AuthorizationHeaderName("Authorization");
        headers->Add(AuthorizationHeaderName, Config_->Token);
        return headers;
    }

    static NJson::TJsonFormatConfigPtr MakeJsonFormatConfig()
    {
        auto config = New<NJson::TJsonFormatConfig>();
        config->EncodeUtf8 = false; // Hipsters use real Utf8.
        return config;
    }

    TString OnTvmCallResult(
        const TGuid& callId,
        const TString& serviceId,
        const NProfiling::TWallTimer& timer,
        const TErrorOr<IResponsePtr>& rspOrError)
    {
        Profiler_.Update(CallTimeGauge_, timer.GetElapsedValue());

        auto onError = [&] (TError error) {
            error.Attributes().Set("call_id", callId);
            Profiler_.Increment(FailedCallCountCounter_);
            YT_LOG_DEBUG(error);
            THROW_ERROR(error);
        };

        if (!rspOrError.IsOK()) {
            onError(TError("TVM call failed")
                << rspOrError);
        }

        const auto& rsp = rspOrError.Value();
        if (rsp->GetStatusCode() != EStatusCode::OK) {
            onError(TError("TVM call returned HTTP status code %v",
                static_cast<int>(rsp->GetStatusCode())));
        }

        INodePtr rootNode;
        try {

            YT_LOG_DEBUG("Started reading response body from TVM (CallId: %v)",
            callId);

            auto body = rsp->ReadAll();

            YT_LOG_DEBUG("Finished reading response body from TVM (CallId: %v)\n%v",
                callId,
                body);

            TMemoryInput stream(body.Begin(), body.Size());
            auto factory = NYTree::CreateEphemeralNodeFactory();
            auto builder = NYTree::CreateBuilderFromFactory(factory.get());
            static const auto Config = MakeJsonFormatConfig();
            NJson::ParseJson(&stream, builder.get(), Config);
            rootNode = builder->EndTree();

            YT_LOG_DEBUG("Parsed TVM daemon reply (CallId: %v)",
                callId);
        } catch (const std::exception& ex) {
            onError(TError(
                "Error parsing TVM response")
                << ex);
        }

        static const TString ErrorPath("/error");
        auto errorNode = FindNodeByYPath(rootNode, ErrorPath);
        if (errorNode) {
            onError(TError("TVM daemon returned an error")
                << TErrorAttribute("message", errorNode->GetValue<TString>()));
        }

        TString ticket;
        try {
            auto ticketPath = "/" + ToYPathLiteral(serviceId) + "/ticket";
            ticket = GetNodeByYPath(rootNode, ticketPath)->GetValue<TString>();
            Profiler_.Increment(SuccessfulCallCountCounter_);
        } catch (const std::exception& ex) {
            onError(TError("Error parsing TVM daemon reply")
                << ex);
        }

        return ticket;
    }
};

ITvmServicePtr CreateDefaultTvmService(
    TDefaultTvmServiceConfigPtr config,
    IPollerPtr poller,
    NProfiling::TProfiler profiler)
{
    return New<TDefaultTvmService>(
        std::move(config),
        std::move(poller),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
