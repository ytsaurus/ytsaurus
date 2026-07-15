#include "url_downloader_functions.h"

#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/invoker_util.h>

#include <util/random/random.h>

#include <algorithm>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

void TUrlMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("host", &TThis::Host)
        .Default();
    registrar.Parameter("url", &TThis::Url)
        .Default();
}

void TProcessedUrlMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("host", &TThis::Host)
        .Default();
    registrar.Parameter("url", &TThis::Url)
        .Default();
    registrar.Parameter("data", &TThis::Data)
        .Default();
}

void TLimitedHostState::Register(TRegistrar registrar)
{
    registrar.Parameter("host", &TThis::Host)
        .Default();
    registrar.Parameter("urls", &TThis::Urls)
        .Default();
}

void TDynamicLimitedUrlDownloadParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("check_host_period", &TThis::CheckHostPeriod)
        .Default(TDuration::Seconds(5))
        .GreaterThan(TDuration::Seconds(1));
    registrar.Parameter("persist_limit", &TThis::PersistLimit)
        .Default(1000);
}

////////////////////////////////////////////////////////////////////////////////

// [BEGIN url_downloader]
void TUrlDownloader::RegisterHost(const std::string& host, const std::deque<std::string>& urls)
{
    if (Executors_.contains(host)) {
        return;
    }
    Queue_[host] = urls;

    // Allows to write code without spin locks.
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetCurrentInvoker());

    Executors_[host] = BIND([weakThis = MakeWeak(this), host] {
        // Emulates slow processing with throttle.
        while (true) {
            if (auto strongThis = weakThis.Lock()) {
                if (!strongThis->Queue_[host].empty()) {
                    auto url = strongThis->Queue_[host].front();
                    strongThis->Queue_[host].pop_front();
                    strongThis->ProcessedUrls_[host].push_back({url, strongThis->ProcessUrl(url)});
                }
            } else {
                return;
            }
            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(500 + RandomNumber<ui64>(1000)));
        }
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

void TUrlDownloader::UnregisterHost(const std::string& host)
{
    if (auto iter = Executors_.find(host); iter != Executors_.end()) {
        auto future = iter->second;
        Executors_.erase(iter);
        future.Cancel(TError("Host unregistered"));
        Y_UNUSED(NConcurrency::WaitFor(future));
    }
    ProcessedUrls_.erase(host);
    Queue_.erase(host);
}

void TUrlDownloader::RegisterUrl(const std::string& host, const std::string& url)
{
    YT_VERIFY(Executors_.contains(host));
    Queue_[host].push_back(url);
}

std::deque<std::pair<std::string, std::string>> TUrlDownloader::ExtractProcessedUrls(const std::string& host)
{
    std::deque<std::pair<std::string, std::string>> processed;
    std::swap(processed, ProcessedUrls_[host]);
    return processed;
}

std::string TUrlDownloader::ProcessUrl(const std::string& url) const
{
    int digits = 0;
    for (const char ch : url) {
        if (ch >= '0' && ch <= '9') {
            digits += 1;
        }
    }
    return Format("length: %v, digits: %v", std::ssize(url), digits);
}

// [END url_downloader]

////////////////////////////////////////////////////////////////////////////////

// [BEGIN limited_url_download]
void TLimitedUrlDownloadFunction::Init(const IRuntimeInitContextPtr& initContext)
{
    initContext->InitClient<TLimitedHostState>(HostStateClient_, "host_state");
    UrlDownloader_ = New<TUrlDownloader>();
}

void TLimitedUrlDownloadFunction::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto request = context->ConvertToYsonMessage<TUrlMessage>(message);
    auto state = HostStateClient_.GetState(message->Key);
    state->Host = request->Host;
    state->Urls.push_back(request->Url);
    UrlDownloader_->RegisterHost(state->Host, state->Urls);
    UrlDownloader_->RegisterUrl(request->Host, request->Url);
    output->AddTimer(GetNextHostCheck(state->Host, context));
    EnforceLimit(state, context);
}

void TLimitedUrlDownloadFunction::ProcessTimer(
    const TInputTimerConstPtr& timer,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto state = HostStateClient_.GetState(timer->Key);
    EnforceLimit(state, context);
    UrlDownloader_->RegisterHost(state->Host, state->Urls);
    for (const auto& [url, data] : UrlDownloader_->ExtractProcessedUrls(state->Host)) {
        if (auto iter = std::find(state->Urls.begin(), state->Urls.end(), url); iter != state->Urls.end()) {
            state->Urls.erase(iter);
        }
        auto processedUrl = New<TProcessedUrlMessage>();
        processedUrl->Host = state->Host;
        processedUrl->Url = url;
        processedUrl->Data = data;
        output->AddMessage(context->ConvertToMessage(processedUrl));
    }
    if (state->Urls.empty()) {
        UrlDownloader_->UnregisterHost(state->Host);
        state.Clear();
    } else {
        output->AddTimer(GetNextHostCheck(state->Host, context));
    }
}

TSystemTimestamp TLimitedUrlDownloadFunction::GetNextHostCheck(
    const std::string& host,
    const IRuntimeContextPtr& context) const
{
    auto parameters = context->GetDynamicParameters<TDynamicLimitedUrlDownloadParameters>();
    ui64 period = parameters->CheckHostPeriod.Seconds();
    ui64 shift = THash<std::string>()(host) % period;
    ui64 timestamp = context->GetCurrentTimestamp().Underlying();
    ui64 nextTimestamp = timestamp - timestamp % period + shift + period;
    return TSystemTimestamp{nextTimestamp};
}

void TLimitedUrlDownloadFunction::EnforceLimit(
    const TStateAccessor<TLimitedHostState>& state,
    const IRuntimeContextPtr& context) const
{
    auto persistLimit = context->GetDynamicParameters<TDynamicLimitedUrlDownloadParameters>()->PersistLimit;
    while (std::ssize(state->Urls) > persistLimit) {
        state->Urls.pop_front();
    }
}

// [END limited_url_download]

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
