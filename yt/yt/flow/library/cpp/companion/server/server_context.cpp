#include "server_context.h"

#include <yt/yt/flow/library/cpp/companion/config.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/http/client.h>

#include <yt/yt/core/https/client.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

TCompanionServerContextPtr CreateCompanionServerContext(
    const NCompanion::TCompanionExecutionConfigPtr& config,
    IInvokerPtr invoker)
{
    auto context = New<TCompanionServerContext>();
    context->Invoker = std::move(invoker);
    context->HttpPoller = NConcurrency::CreateThreadPoolPoller(config->HttpPollerThreads, "CompanionHttp");
    context->HttpClient = NHttp::CreateClient(config->HttpClientConfig, context->HttpPoller);
    context->HttpsClient = NHttps::CreateClient(config->HttpsClientConfig, context->HttpPoller);
    return context;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
