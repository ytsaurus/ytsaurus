#include "solomon_agent_client.h"

#include <yql/essentials/utils/log/log.h>
#include <yql/cfg/proto/worker_config.pb.h>
#include <yql/essentials/providers/common/metrics/protos/metrics_registry.pb.h>

#include <library/cpp/json/json_writer.h>

#include <library/cpp/monlib/encode/encoder.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/metrics/metric_registry.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/http/misc/http_headers.h>

#include <library/cpp/retry/retry.h>

#include <util/system/hostname.h>
#include <util/thread/pool.h>

namespace NYql {
namespace {

class TAbstractMetricSerializer : public IMetricsSerializer {
public:
    using TLabelList = TSolomonAgentClient::TLabelList;

    // Derivs are not supported by solomon.yandex.net push api but
    // supported by solomon agent push api
    explicit TAbstractMetricSerializer()
    {
        Reset();
    }

    ~TAbstractMetricSerializer() override = default;

    void Reset() {
        Encoder_ = NMonitoring::EncoderSpackV1(&Str_,
                                               NMonitoring::ETimePrecision::SECONDS,
                                               NMonitoring::ECompression::ZSTD);
        Encoder_->OnStreamBegin();
        Str_.Clear();
        Groups_  = 0;
        Labels_ = 0;
        LabelStack_.clear();
        Counters_ = 0;
        Histograms_ = 0;
    }

    void WriteCommonLabels(const TLabelList& labels) override {
        Encoder_->OnLabelsBegin();
        for (const auto& [name, value] : labels) {
            Encoder_->OnLabel(name, value);
        }
        Encoder_->OnLabelsEnd();
    }

    void UseCommonLabels(const TLabelList& labels) {
        for (const auto& [name, value] : labels) {
            PushLabel(name, value);
        }
    }

    TString Finish() override {
        Encoder_->OnStreamEnd();
        Encoder_->Close();
        Str_.Flush();

        auto result = std::move(Str_.Str());

        YQL_LOG(DEBUG)
            << "Groups: " << Groups_ << " "
            << "Labels: " << Labels_ << " "
            << "Counters: " << Counters_ << " "
            << "Histograms: " << Histograms_;

        Reset();
        return result;
    }

    void OnGroupBegin(const TString& labelName, const TString& labelValue,
                      const NMonitoring::TDynamicCounters*) override {
        if (!labelName.empty() || !labelValue.empty()) {
            PushLabel(labelName, labelValue);
        }
        Groups_ ++;
    }

    void OnGroupEnd(const TString& labelName, const TString& labelValue,
                    const NMonitoring::TDynamicCounters*) override {
        if (!labelName.empty() || !labelValue.empty()) {
            PopLabel();
        }
    }

    NMonitoring::TCountableBase::EVisibility Visibility() const override {
        return NMonitoring::TCountableBase::EVisibility::Public;
    }

    void Consume(const TSensorsGroupPtr& sensors) override {
        sensors->Accept(TString(), TString(), *this);
    }

    void Consume(const NProto::TMetricsRegistrySnapshot& snapshot) override {
        if (snapshot.HasRootGroup()) {
            Consume(snapshot.GetRootGroup());
        }
    }

protected:
    TStringStream Str_;
    NMonitoring::IMetricEncoderPtr Encoder_;
    TLabelList LabelStack_{};
    int Groups_ = 0;
    int Labels_ = 0;
    int Counters_ = 0;
    int Histograms_ = 0;

    void PushLabel(TString name, TString value) {
        LabelStack_.emplace_back(std::move(name), std::move(value));
        Labels_ ++;
    }

    void PopLabel() {
        LabelStack_.pop_back();
    }

    void SerializeLabels(const TString& lastName, const TString& lastValue) {
        Encoder_->OnLabelsBegin();

        TMap<TStringBuf, TStringBuf> deduplicatedLabels;
        for (const auto& [name, value] : LabelStack_) {
            deduplicatedLabels[name] = value;
        }
        deduplicatedLabels[lastName] = lastValue;

        for (const auto& [name, value] : deduplicatedLabels) {
            Encoder_->OnLabel(name, value);
        }

        Encoder_->OnLabelsEnd();
    }

    void Consume(const NProto::TCounterGroup& counterGroup) {
        if (counterGroup.HasLabel()) {
            OnGroupBegin(counterGroup.GetLabel().GetName(), counterGroup.GetLabel().GetValue(), nullptr);
        }

        for (const auto& counter : counterGroup.GetCounters()) {
            Consume(counter);
        }
        for (const auto& subgroup : counterGroup.GetGroups()) {
            Consume(subgroup);
        }

        if (counterGroup.HasLabel()) {
            OnGroupEnd(counterGroup.GetLabel().GetName(), counterGroup.GetLabel().GetValue(), nullptr);
        }
    }

    void Consume(const NProto::TCounter& counter) {
        if (counter.GetBucket().empty()) {
            ConsumeAsSinglePoint(counter);
        } else {
            ConsumeAsHistogram(counter);
        }
    }

    void ConsumeAsHistogram(const NProto::TCounter& counter) {
        size_t bucketsCount = counter.GetBucket().size();
        auto hist = NMonitoring::TExplicitHistogramSnapshot::New(bucketsCount);
        for (size_t i = 0; i < bucketsCount; ++i) {
            (*hist)[i] = {counter.GetBucket(i).GetUpperBound(), counter.GetBucket(i).GetValue()};
        }
        bool derivative = counter.HasDerivative() && counter.GetDerivative();
        OnHistogram(counter.GetLabel().GetName(), counter.GetLabel().GetValue(), std::move(hist), derivative);
    }

    void ConsumeAsSinglePoint(const NProto::TCounter& counter) {
        bool derivative = counter.HasDerivative() && counter.GetDerivative();
        NMonitoring::TCounterForPtr counterForPtr(derivative);
        counterForPtr = counter.GetValue();

        OnCounter(counter.GetLabel().GetName(), counter.GetLabel().GetValue(), &counterForPtr);
    }
};

class TMetricSerializer: public TAbstractMetricSerializer {
public:
    explicit TMetricSerializer(bool useDerivsForCounters, bool useDerivsForHistograms)
        : TAbstractMetricSerializer()
        , UseDerivsForCounters_(useDerivsForCounters)
        , UseDerivsForHistograms_(useDerivsForHistograms)
    { }

    void OnCounter(const TString& labelName, const TString& labelValue,
                   const NMonitoring::TCounterForPtr* counter) override {
        auto type = (UseDerivsForCounters_ && counter->ForDerivative()) ? NMonitoring::EMetricType::RATE : NMonitoring::EMetricType::GAUGE;
        Counters_ ++;
        Encoder_->OnMetricBegin(type);
        SerializeLabels(labelName, labelValue);
        Encoder_->OnInt64(TInstant::Zero(), counter->Val());
        Encoder_->OnMetricEnd();
    }

    void OnHistogram(const TString& labelName, const TString& labelValue,
                     NMonitoring::IHistogramSnapshotPtr snapshot, bool derivative) override {
        auto type = (UseDerivsForHistograms_ && derivative) ? NMonitoring::EMetricType::HIST_RATE : NMonitoring::EMetricType::HIST;
        Histograms_ ++;
        Encoder_->OnMetricBegin(type);
        SerializeLabels(labelName, labelValue);
        Encoder_->OnHistogram(TInstant::Zero(), snapshot);
        Encoder_->OnMetricEnd();
    }

private:
    bool UseDerivsForCounters_;
    bool UseDerivsForHistograms_;
};

class TMetricSerializerAndDiff: public TAbstractMetricSerializer {
private:
    struct TPusherState {
        THashMap<TString, i64> OldCounters;
        THashMap<TString, TVector<i64>> OldBuckets;
    };

    class TDiffSnapshot: public NMonitoring::IHistogramSnapshot
    {
    public:
        TDiffSnapshot(TVector<i64>& oldBuckets, NMonitoring::IHistogramSnapshotPtr& newBuckets)
            : OldBuckets(oldBuckets)
            , NewBuckets(newBuckets)
        { }

        ui32 Count() const override {
            return NewBuckets->Count();
        }

        NMonitoring::TBucketBound UpperBound(ui32 index) const override {
            return NewBuckets->UpperBound(index);
        }

        NMonitoring::TBucketValue Value(ui32 index) const override {
            return NewBuckets->Value(index) - OldBuckets[index];
        }

    private:
        TVector<i64> OldBuckets;
        NMonitoring::IHistogramSnapshotPtr NewBuckets;
    };

public:
    void OnCounter(const TString& labelName, const TString& labelValue,
                   const NMonitoring::TCounterForPtr* counter) override
    {
        auto type = NMonitoring::EMetricType::GAUGE;
        Encoder_->OnMetricBegin(type);
        Counters_ ++;
        if (counter->ForDerivative()) {
            auto key = SerializeLabels(labelName, labelValue);
            auto& oldValue = State.OldCounters[key];
            auto newValue = counter->Val();
            Encoder_->OnInt64(TInstant::Zero(), newValue - oldValue);
            oldValue = newValue;
        } else {
            TAbstractMetricSerializer::SerializeLabels(labelName, labelValue);
            Encoder_->OnInt64(TInstant::Zero(), counter->Val());
        }
        Encoder_->OnMetricEnd();
    }

    void OnHistogram(const TString& labelName, const TString& labelValue,
                     NMonitoring::IHistogramSnapshotPtr snapshot, bool derivative) override
    {
        auto type = NMonitoring::EMetricType::HIST;
        Encoder_->OnMetricBegin(type);
        Histograms_ ++;
        if (derivative) {
            auto key = SerializeLabels(labelName, labelValue);
            auto& oldBuckets = State.OldBuckets[key];
            oldBuckets.resize(snapshot->Count());
            Encoder_->OnHistogram(TInstant::Zero(), new TDiffSnapshot(oldBuckets, snapshot));
            for (ui32 i = 0; i < snapshot->Count(); i ++) {
                oldBuckets[i] = snapshot->Value(i);
            }
        } else {
            TAbstractMetricSerializer::SerializeLabels(labelName, labelValue);
            Encoder_->OnHistogram(TInstant::Zero(), snapshot);
        }
        Encoder_->OnMetricEnd();
    }

private:
    TStringBuf SerializeLabels(const TString& lastName, const TString& lastValue) {
        Encoder_->OnLabelsBegin();
        TMap<TStringBuf, TStringBuf> deduplicatedLabels;
        for (const auto& [name, value] : LabelStack_) {
            deduplicatedLabels[name] = value;
        }
        deduplicatedLabels[lastName] = lastValue;

        Buf.clear();
        for (const auto& [name, value] : deduplicatedLabels) {
            Encoder_->OnLabel(name, value);
            Buf << name << value;
        }

        Encoder_->OnLabelsEnd();
        return Buf;
    }

    TPusherState State;
    TStringBuilder Buf;
};

struct TMetricPusher : public IObjectInQueue {
    TSolomonAgentConfig Config;
    TString Data;

    std::unique_ptr<TKeepAliveHttpClient> Client;

    explicit TMetricPusher(TSolomonAgentConfig config, TString&& data)
        : Config(std::move(config))
        , Data(std::move(data))
    { }

    TString MakePath() const {
        TStringBuilder sb;
        sb  << Config.Path
            << "?project=" << Config.Project
            << "&service=" << Config.Service
            << "&cluster=" << Config.Cluster;
        YQL_LOG(DEBUG) << "POST: " << sb;
        return sb;
    }

    void CreateClientIfNeeded() {
        if (!Client) {
            YQL_LOG(DEBUG) << "Create client: " << Config.Server << ":" << Config.Port;
            Client = std::make_unique<TKeepAliveHttpClient>(
                Config.Server,
                Config.Port,
                Config.TimeoutSocket,
                Config.TimeoutConnection);
        }
    }

    void Process(void*) override {
        TSimpleHttpClient::THeaders headers;
        headers[NHttpHeaders::CONTENT_TYPE] = NMonitoring::NFormatContenType::SPACK;
        if (Config.Authorization) {
            headers[NHttpHeaders::AUTHORIZATION] = Config.Authorization;
        }

        const auto retryOpts = TRetryOptions().WithCount(Config.RetryCount).WithSleep(Config.RetrySleep);
        TStringStream response;

        try {
            auto f = [&] {
                CreateClientIfNeeded();
                auto code = Client->DoPost(MakePath(), Data, &response, headers);
                if (code != 200) {
                    throw THttpRequestException(code);
                }
            };
            std::function<void(const THttpRequestException&)> onFail = [&](const THttpRequestException& ex) {
                auto status = ex.GetStatusCode();
                Client.reset();
                if (status >= 400 && status < 500) {
                    throw ex;
                }
            };
            DoWithRetry(f, onFail, retryOpts, true);
        } catch (yexception& e) {
            YQL_LOG(ERROR) << "Failed to push metrics to solomon agent: " << e.what()
                << "\nServer response: " << response.Str();
        }
    }
};

} // namespace


TSolomonAgentConfig& TSolomonAgentConfig::WithServer(const TString& server) {
    Server = server;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithPath(const TString& path) {
    Path = path;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithPort(ui16 port) {
    Port = port;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithProject(const TString& project) {
    Project = project;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithCluster(const TString& cluster) {
    Cluster = cluster;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithService(const TString& service) {
    Service = service;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithHost(const TString& host) {
    Host = host;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithRetryCount(ui64 retryCount) {
    RetryCount = retryCount;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithRetrySleep(const TDuration& retrySleep) {
    RetrySleep = retrySleep;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithTimeoutSocket(const TDuration& timeoutSocket) {
    TimeoutSocket = timeoutSocket;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithTimeoutConnection(const TDuration& timeoutConnection) {
    TimeoutConnection = timeoutConnection;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithUpdatePeriod(const TDuration& updatePeriod) {
    UpdatePeriod = updatePeriod;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithCommonLabels(const TSolomonAgentClient::TLabelList& commonLabels) {
    CommonLabels = commonLabels;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithMetricsVersion(ui64 metricsVersion) {
    MetricsVersion = metricsVersion;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithDerivs(bool derivs) {
    HistogramDerivs = CounterDerivs = derivs;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithDerivsForCounters(bool derivs) {
    CounterDerivs = derivs;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithDerivsForHistograms(bool derivs) {
    HistogramDerivs = derivs;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithCalcDerivs(bool calcDerivs) {
    CalcDerivs = calcDerivs;
    return *this;
}

TSolomonAgentConfig& TSolomonAgentConfig::WithAuthorizaton(const TString& authorization) {
    Authorization = authorization;
    return *this;
}

struct TSolomonAgentClient::TImpl {
    using TLabelList = TSolomonAgentClient::TLabelList;

    TSolomonAgentConfig Config;
    std::unique_ptr<IMetricsSerializer> Serializer;
    TThreadPool Executor;

    explicit TImpl(TSolomonAgentConfig config)
        : Config(std::move(config))
        , Serializer(CreateMetricSerializer(Config))
    {
        if (!Config.Host.empty()) {
            AddCommonLabel("host", Config.Host);
        }
        AddCommonLabel("metrics_version", TStringBuilder{} << Config.MetricsVersion);
        UseOwnThread(false);
    }

    void UseOwnThread(bool flag) {
        Executor.Start(flag ? 1 : 0);
    }

    template <class TArg>
    void Send(TArg&& arg)
    {
        try {
            YQL_LOG(DEBUG) << "Collecting sensors";
            Serializer->WriteCommonLabels(Config.CommonLabels); // isn't supported yet by unified_agent
            // Serializer.UseCommonLabels(Config.CommonLabels);
            Serializer->Consume(std::forward<TArg>(arg));
            auto result = Serializer->Finish();
            auto pusher = MakeHolder<TMetricPusher>(Config, std::move(result));

            YQL_LOG(DEBUG) << "Sending sensors";
            Executor.SafeAddAndOwn(std::move(pusher));
        } catch (...) {
            YQL_LOG(ERROR) << "Error sending metrics: " << CurrentExceptionMessage();
        }
    }

    void SendMetrics(const TSensorsGroupPtr& sensors)
    {
        Send(sensors);
    }

    const TSolomonAgentConfig& GetConfig() const
    {
        return Config;
    }

    TSolomonAgentConfig& MutableConfig()
    {
        return Config;
    }

    TDuration GetUpdatePeriod() const
    {
        return Config.UpdatePeriod;
    }

    void SetUpdatePeriod(const TDuration& updatePeriod)
    {
        Config.UpdatePeriod = updatePeriod;
    }

    TDuration GetDurationUntilNextGridPoint() const
    {
        auto now = Now();
        auto result = Config.UpdatePeriod.MicroSeconds() - now.MicroSeconds() % Config.UpdatePeriod.MicroSeconds();
        return TDuration::MicroSeconds(result);
    }

    void SendMetricsRegistrySnapshot(NProto::TMetricsRegistrySnapshot&& snapshot)
    {
        Send(std::move(snapshot));
    }

    TString GetConfigAsJSON() const {
        TStringStream str;
        NJson::TJsonWriter writer(&str, false);
        writer.OpenMap();
        writer.Write("Server", Config.Server);
        writer.Write("Path", Config.Path);
        writer.Write("Port", Config.Port);

        writer.Write("Project", Config.Project);
        writer.Write("Cluster", Config.Cluster);
        writer.Write("Service", Config.Service);
        writer.Write("Host", Config.Host);

        writer.Write("RetryCount", Config.RetryCount);
        writer.Write("RetrySleep", Config.RetrySleep.ToString());
        writer.Write("TimeoutSocket", Config.TimeoutSocket.ToString());
        writer.Write("TimeoutConnection", Config.TimeoutConnection.ToString());

        writer.Write("UpdatePeriod", Config.UpdatePeriod.ToString());

        writer.WriteKey("CommonLabels");
        writer.OpenMap();
        for (const auto& [name, value] : Config.CommonLabels) {
            writer.Write(name, value);
        }
        writer.CloseMap();

        writer.Write("MetricsVersion", Config.MetricsVersion);

        writer.CloseMap();
        writer.Flush();

        return str.Str();
    }

    void AddCommonLabel(TString labelName, TString labelValue) {
        Config.CommonLabels.emplace_back(std::move(labelName), std::move(labelValue));
    }
};

TSolomonAgentClient::TSolomonAgentClient(const TSolomonAgentConfig& config)
    : Impl_(new TImpl(config))
{
}

TSolomonAgentClient::~TSolomonAgentClient() = default;

void TSolomonAgentClient::SendMetrics(const NYql::TSensorsGroupPtr& sensors) {
    Impl_->SendMetrics(sensors);
}

const TSolomonAgentConfig& TSolomonAgentClient::GetConfig() const {
    return Impl_->GetConfig();
}

TSolomonAgentConfig& TSolomonAgentClient::MutableConfig() {
    return Impl_->MutableConfig();
}

TDuration TSolomonAgentClient::GetUpdatePeriod() const {
    return Impl_->GetUpdatePeriod();
}

void TSolomonAgentClient::SetUpdatePeriod(const TDuration &updatePeriod) {
    Impl_->SetUpdatePeriod(updatePeriod);
}

TDuration TSolomonAgentClient::GetDurationUntilNextGridPoint() const {
    return Impl_->GetDurationUntilNextGridPoint();
}

TString TSolomonAgentClient::GetConfigAsJSON() const {
    return Impl_->GetConfigAsJSON();
}

void TSolomonAgentClient::SendMetricsRegistrySnapshot(NProto::TMetricsRegistrySnapshot&& snapshot) {
    Impl_->SendMetricsRegistrySnapshot(std::move(snapshot));
}

void TSolomonAgentClient::UseOwnThread(bool flag) {
    Impl_->UseOwnThread(flag);
}

void TSolomonAgentClient::AddCommonLabel(const TString& labelName, const TString& labelValue) {
    Impl_->AddCommonLabel(labelName, labelValue);
}

void TSolomonAgentClient::AddPIDLabel() {
    AddCommonLabel("pid", IntToString<10>(getpid()));
}

TSolomonAgentClientPtr BuildSolomonAgentClient(const TMaybe<NProto::TSolomonAgentClientConfig>& config) {
    if (config) {
        return MakeHolder<TSolomonAgentClient>(ParseSolomonAgentConfig(*config));
    } else {
        return nullptr;
    }
}

TSolomonAgentConfig ParseSolomonAgentConfig(const NProto::TSolomonAgentClientConfig& config) {
    TSolomonAgentConfig result;
    const auto& conn = config.GetConnection();

    SplitUrlToHostAndPath(conn.GetEndPoint(), result.Server, result.Path);
    if (conn.HasPort()) {
        result.WithPort(conn.GetPort());
    }

    result
        .WithProject(conn.GetProject())
        .WithCluster(conn.GetCluster())
        .WithService(conn.GetService());

    if (conn.HasHost()) {
        result.WithHost(conn.GetHost());
    } else {
        TString fullHostName = HostName();
        // e.g. kikimr0002.search.yandex.net -> kikimr0002
        size_t pos = fullHostName.find('.');
        if (pos != TString::npos) {
            fullHostName.resize(pos);
        }
        result.WithHost(fullHostName);
    }

    if (conn.HasRetryCount()) {
        result.WithRetryCount(conn.GetRetryCount());
    }

    if (conn.HasRetrySleepMs()) {
        result.WithRetrySleep(TDuration::MilliSeconds(conn.GetRetrySleepMs()));
    }

    if (conn.HasTimeoutSocketMs()) {
        result.WithTimeoutSocket(TDuration::MilliSeconds(conn.GetTimeoutSocketMs()));
    }

    if (conn.HasTimeoutConnectionMs()) {
        result.WithTimeoutConnection(TDuration::MilliSeconds(conn.GetTimeoutConnectionMs()));
    }

    result.WithUpdatePeriod(TDuration::MilliSeconds(config.GetUpdatePeriodMs()));

    result.CommonLabels.reserve(config.GetCommonLabels().size());
    for (const auto& label : config.GetCommonLabels()) {
        result.CommonLabels.emplace_back(label.GetName(), label.GetValue());
    }

    result.WithMetricsVersion(config.GetMetricsVersion());

    return result;
}

TMaybe<NProto::TSolomonAgentClientConfig> GetSolomonAgentConfig(const NProto::TWorkerConfig& worker) {
    return worker.HasSolomonAgentClient() ? MakeMaybe(worker.GetSolomonAgentClient()) : Nothing();
}

std::unique_ptr<IMetricsSerializer> CreateMetricSerializer(const TSolomonAgentConfig& config) {
    return config.CalcDerivs ?
        std::unique_ptr<IMetricsSerializer>(new TMetricSerializerAndDiff()) :
        std::unique_ptr<IMetricsSerializer>(new TMetricSerializer(config.CounterDerivs, config.HistogramDerivs));
}

} // namespace NYql
