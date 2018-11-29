#include "default_tvm_service.h"
#include "tvm_service.h"
#include "config.h"
#include "helpers.h"
#include "private.h"

#include <yt/core/json/json_parser.h>

#include <yt/core/ytree/ypath_client.h>

#include <yt/core/ypath/token.h>

#include <library/http/simple/http_client.h>

namespace NYT {
namespace NAuth {

using namespace NYTree;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TDefaultTvmService
    : public ITvmService
{
public:
    TDefaultTvmService(
        TDefaultTvmServiceConfigPtr config,
        IInvokerPtr invoker)
        : Config_(std::move(config))
        , Invoker_(std::move(invoker))
    { }

    virtual TFuture<TString> GetTicket(const TString& serviceId) override
    {
        LOG_DEBUG("Retrieving TVM ticket (ServiceId: %v)",
            serviceId);

        auto deadline = TInstant::Now() + Config_->RequestTimeout;
        return BIND(&TDefaultTvmService::DoGetTicket, MakeStrong(this), serviceId, deadline)
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    const TDefaultTvmServiceConfigPtr Config_;
    const IInvokerPtr Invoker_;

private:
    TString DoGetTicket(const TString& serviceId, TInstant deadline)
    {
        TSafeUrlBuilder builder;
        builder.AppendString(AsStringBuf("/tvm/tickets?"));
        builder.AppendParam(AsStringBuf("dsts"), serviceId);
        builder.AppendString("&format=json");

        auto safeUrl = builder.FlushSafeUrl();
        auto realUrl = builder.FlushRealUrl();

        auto result = DoCallOnce(
            realUrl,
            safeUrl,
            deadline);

        static const TString ErrorPath("/error");
        auto errorNode = FindNodeByYPath(result, ErrorPath);
        if (errorNode) {
            THROW_ERROR_EXCEPTION("TVM daemon returned an error")
                << TErrorAttribute("message", errorNode->GetValue<TString>());
        }

        try {
            auto ticketPath = "/" + ToYPathLiteral(serviceId) + "/ticket";
            return GetNodeByYPath(result, ticketPath)->GetValue<TString>();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing TVM daemon reply")
                << ex;
        }
    }

    INodePtr DoCallOnce(
        const TString& realUrl,
        const TString& safeUrl,
        TInstant deadline)
    {
        auto callId = TGuid::Create();

        TString resultString;
        INodePtr resultNode;

        LOG_DEBUG("Calling TVM daemon (Url: %v, CallId: %v)",
            safeUrl,
            callId);

        {
            auto timeout = deadline - TInstant::Now();
            static const TString Host("localhost");
            TSimpleHttpClient httpClient(Host, Config_->Port, timeout, timeout);
            TSimpleHttpClient::THeaders headers{
                {"Authorization", Config_->Token}
            };
            TStringOutput outputStream(resultString);
            httpClient.DoGet(realUrl, &outputStream, headers);
        }

        LOG_DEBUG("Received TVM daemon reply (CallId: %v)\n%v",
            callId,
            resultString);

        {
            TStringInput inputStream(resultString);
            auto factory = NYTree::CreateEphemeralNodeFactory();
            auto builder = NYTree::CreateBuilderFromFactory(factory.get());
            auto config = New<NJson::TJsonFormatConfig>();
            config->EncodeUtf8 = false; // Hipsters use real Utf8.
            NJson::ParseJson(&inputStream, builder.get(), std::move(config));
            resultNode = builder->EndTree();
        }

        LOG_DEBUG("Parsed TVM daemon reply (CallId: %v)",
            callId);

        return resultNode;
    }
};

ITvmServicePtr CreateDefaultTvmService(
    TDefaultTvmServiceConfigPtr config,
    IInvokerPtr invoker)
{
    return New<TDefaultTvmService>(
        std::move(config),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
