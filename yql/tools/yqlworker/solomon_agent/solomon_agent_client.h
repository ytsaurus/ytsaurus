#pragma once

#include <yql/essentials/providers/common/metrics/sensors_group.h>

#include <library/cpp/monlib/metrics/metric_consumer.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NYql {
namespace NProto {
    class TMetricsRegistrySnapshot;
    class TSolomonAgentConnectionConfig;
    class TSolomonAgentClientConfig;
    class TWorkerConfig;
} // namespace NProto

struct TSolomonAgentConfig;

class TSolomonAgentClient {
public:
    using TLabelList = TVector<std::pair<TString, TString>>;

    explicit TSolomonAgentClient(const TSolomonAgentConfig& config);
    ~TSolomonAgentClient();

    void UseOwnThread(bool flag = true);

    void SendMetrics(const TSensorsGroupPtr& sensors);
    void SendMetricsRegistrySnapshot(NProto::TMetricsRegistrySnapshot&& snapshot);

    const TSolomonAgentConfig& GetConfig() const;
    TSolomonAgentConfig& MutableConfig();

    TDuration GetUpdatePeriod() const;
    void SetUpdatePeriod(const TDuration& updatePeriod);
    TDuration GetDurationUntilNextGridPoint() const;

    TString GetConfigAsJSON() const;

    void AddCommonLabel(const TString& labelName, const TString& labelValue);
    void AddPIDLabel();

private:
    struct TImpl;
    THolder<TImpl> Impl_;
};

struct TSolomonAgentConfig {
    TString Server;
    TString Path;
    ui16 Port = 80;

    TString Project;
    TString Cluster;
    TString Service;
    TString Host;

    ui64 RetryCount = 3;
    TDuration RetrySleep = TDuration::Seconds(3);
    TDuration TimeoutSocket = TDuration::Seconds(5);
    TDuration TimeoutConnection = TDuration::Seconds(30);

    TDuration UpdatePeriod = TDuration::Seconds(15);

    TSolomonAgentClient::TLabelList CommonLabels;

    ui64 MetricsVersion;
    bool HistogramDerivs = true;
    bool CounterDerivs = true;
    bool CalcDerivs = false;
    TString Authorization;

    TSolomonAgentConfig& WithServer(const TString& server);
    TSolomonAgentConfig& WithPath(const TString& path);
    TSolomonAgentConfig& WithPort(ui16 port);
    TSolomonAgentConfig& WithProject(const TString& project);
    TSolomonAgentConfig& WithCluster(const TString& cluster);
    TSolomonAgentConfig& WithService(const TString& service);
    TSolomonAgentConfig& WithHost(const TString& host);
    TSolomonAgentConfig& WithRetryCount(ui64 retryCount);
    TSolomonAgentConfig& WithRetrySleep(const TDuration& retrySleep);
    TSolomonAgentConfig& WithTimeoutSocket(const TDuration& timeoutSocket);
    TSolomonAgentConfig& WithTimeoutConnection(const TDuration& timeoutConnection);
    TSolomonAgentConfig& WithUpdatePeriod(const TDuration& updatePeriod);
    TSolomonAgentConfig& WithCommonLabels(const TSolomonAgentClient::TLabelList& commonLabels);
    TSolomonAgentConfig& WithMetricsVersion(ui64 metricsVersion);
    TSolomonAgentConfig& WithDerivs(bool derivs);
    TSolomonAgentConfig& WithDerivsForCounters(bool derivs);
    TSolomonAgentConfig& WithDerivsForHistograms(bool derivs);
    TSolomonAgentConfig& WithCalcDerivs(bool calcDerivs);
    TSolomonAgentConfig& WithAuthorizaton(const TString& authorizaton);
};

struct IMetricsSerializer: public NMonitoring::ICountableConsumer {
    virtual ~IMetricsSerializer() = default;

    virtual void WriteCommonLabels(const TSolomonAgentClient::TLabelList& labels) = 0;

    virtual void Consume(const TSensorsGroupPtr& sensors) = 0;

    virtual void Consume(const NProto::TMetricsRegistrySnapshot& snapshot) = 0;

    virtual TString Finish(void) = 0;
};

using TSolomonAgentClientPtr = THolder<TSolomonAgentClient>;

TSolomonAgentConfig ParseSolomonAgentConfig(const NProto::TSolomonAgentClientConfig& config);
TSolomonAgentClientPtr BuildSolomonAgentClient(const TMaybe<NProto::TSolomonAgentClientConfig>& config);
TMaybe<NProto::TSolomonAgentClientConfig> GetSolomonAgentConfig(const NProto::TWorkerConfig& worker);
std::unique_ptr<IMetricsSerializer> CreateMetricSerializer(const TSolomonAgentConfig& config);

} // namespace NYql
