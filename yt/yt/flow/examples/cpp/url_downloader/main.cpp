#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>
#include <yt/yt/flow/library/cpp/computation/swift_map_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT::NFlow;
using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

struct TUrlMessage
    : public TYsonMessage
{
    std::string Host;
    std::string Url;

    REGISTER_YSON_STRUCT(TUrlMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("host", &TThis::Host)
            .Default();
        registrar.Parameter("url", &TThis::Url)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TUrlMessage);

////////////////////////////////////////////////////////////////////////////////

struct TProcessedUrlMessage
    : public TYsonMessage
{
    std::string Host;
    std::string Url;
    std::string Data;

    REGISTER_YSON_STRUCT(TProcessedUrlMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("host", &TThis::Host)
            .Default();
        registrar.Parameter("url", &TThis::Url)
            .Default();
        registrar.Parameter("data", &TThis::Data)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TProcessedUrlMessage);

////////////////////////////////////////////////////////////////////////////////

// [BEGIN url_downloader]
class TUrlDownloader
    : public NYT::TRefCounted
{
public:
    TUrlDownloader() = default;

    void RegisterHost(const std::string& host, const std::deque<std::string>& urls)
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

    void UnregisterHost(const std::string& host)
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

    void RegisterUrl(const std::string& host, const std::string& url)
    {
        YT_VERIFY(Executors_.contains(host));
        Queue_[host].push_back(url);
    }

    std::deque<std::pair<std::string, std::string>> ExtractProcessedUrls(const std::string& host)
    {
        std::deque<std::pair<std::string, std::string>> processed;
        std::swap(processed, ProcessedUrls_[host]);
        return processed;
    }

    std::string ProcessUrl(const std::string& url) const
    {
        int digits = 0;
        for (const char ch : url) {
            if (ch >= '0' && ch <= '9') {
                digits += 1;
            }
        }
        return Format("length: %v, digits: %v", std::ssize(url), digits);
    }

private:
    THashMap<std::string, TFuture<void>> Executors_;
    THashMap<std::string, std::deque<std::string>> Queue_;
    THashMap<std::string, std::deque<std::pair<std::string, std::string>>> ProcessedUrls_;
};

using TUrlDownloaderPtr = NYT::TIntrusivePtr<TUrlDownloader>;

// [END url_downloader]

////////////////////////////////////////////////////////////////////////////////

// [BEGIN limited_url_download]
struct TLimitedHostState
    : public NYTree::TYsonStruct
{
    std::string Host;
    std::deque<std::string> Urls;

    REGISTER_YSON_STRUCT(TLimitedHostState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("host", &TThis::Host)
            .Default();
        registrar.Parameter("urls", &TThis::Urls)
            .Default();
    }
};

using TLimitedHostStatePtr = NYT::TIntrusivePtr<TLimitedHostState>;

////////////////////////////////////////////////////////////////////////////////

struct TDynamicLimitedUrlDownloadParameters
    : public TTransformComputation::TDynamicParameters
{
    TDuration CheckHostPeriod;

    // Only last PersistLimit urls for each host would be persisted. Older urls may be lost during rebalancing.
    // Otherwise, THostState may became too big to be stored in one row of dynamic table.
    i64 PersistLimit{};

    REGISTER_YSON_STRUCT(TDynamicLimitedUrlDownloadParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("check_host_period", &TThis::CheckHostPeriod)
            .Default(TDuration::Seconds(5))
            .GreaterThan(TDuration::Seconds(1));
        registrar.Parameter("persist_limit", &TThis::PersistLimit)
            .Default(1000);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLimitedUrlDownloadComputation
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicLimitedUrlDownloadParameters);

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitClient(HostStateClient_, "host_state");
        UrlDownloader_ = New<TUrlDownloader>();
    }

    void DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output) override
    {
        const auto ysonMessage = ConvertToYsonMessage(message);
        auto request = ysonMessage->As<TUrlMessage>();
        if (request == nullptr) {
            THROW_ERROR_EXCEPTION("Unexpected message type")
                << TErrorAttribute("stream_id", ysonMessage->Meta->StreamId);
        }
        auto state = HostStateClient_.GetState(message->Key);
        state->Host = request->Host;
        state->Urls.push_back(request->Url);
        UrlDownloader_->RegisterHost(state->Host, state->Urls);
        UrlDownloader_->RegisterUrl(request->Host, request->Url);
        output->AddTimer(GetNextHostCheck(state->Host));
        EnforceLimit(state);
    }

    void DoProcessTimer(const TTimer& timer, IOutputCollectorPtr output) override
    {
        auto state = HostStateClient_.GetState(timer.Key);
        EnforceLimit(state);
        UrlDownloader_->RegisterHost(state->Host, state->Urls);
        for (const auto& [url, data] : UrlDownloader_->ExtractProcessedUrls(state->Host)) {
            if (auto iter = std::find(state->Urls.begin(), state->Urls.end(), url); iter != state->Urls.end()) {
                state->Urls.erase(iter);
            }
            auto processedUrl = New<TProcessedUrlMessage>();
            processedUrl->Host = state->Host;
            processedUrl->Url = url;
            processedUrl->Data = data;
            output->AddMessage(ConvertToMessage(processedUrl));
        }
        if (state->Urls.empty()) {
            UrlDownloader_->UnregisterHost(state->Host);
            state.Clear();
        } else {
            output->AddTimer(GetNextHostCheck(state->Host));
        }
    }

private:
    TMutableStateKeyClient<TLimitedHostState> HostStateClient_;
    TUrlDownloaderPtr UrlDownloader_;

private:
    TSystemTimestamp GetNextHostCheck(const std::string& host) const
    {
        ui64 period = GetDynamicParameters()->CheckHostPeriod.Seconds();
        ui64 shift = THash<std::string>()(host) % period;
        ui64 timestamp = GetCurrentTimestamp().Underlying();
        ui64 nextTimestamp = timestamp - timestamp % period + shift + period;
        return TSystemTimestamp{nextTimestamp};
    }

    void EnforceLimit(const TStateAccessor<TLimitedHostState>& state) const
    {
        while (std::ssize(state->Urls) > GetDynamicParameters()->PersistLimit) {
            state->Urls.pop_front();
        }
    }
};

YT_FLOW_DEFINE_COMPUTATION(TLimitedUrlDownloadComputation);

// [END limited_url_download]

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    builder.RegisterStream<TUrlMessage>("urls");
    builder.RegisterStream<TProcessedUrlMessage>("processed_urls");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
