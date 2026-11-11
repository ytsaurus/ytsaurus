#include "metrics_pusher.h"

#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/http/http_proxy.h>
#include <library/cpp/monlib/encode/format.h>

#include <yql/essentials/utils/log/log.h>

#include <yql/tools/yqlworker/solomon_agent/solomon_agent_client.h>

#include <contrib/ydb/library/yql/utils/actors/http_sender_actor.h>
#include <contrib/ydb/library/yql/utils/actors/http_sender.h>

namespace NYql {
    using namespace NActors;

    namespace {
        auto RetryPolicy = NYql::NDq::THttpSenderRetryPolicy::GetExponentialBackoffPolicy(
            [](const NHttp::TEvHttpProxy::TEvHttpIncomingResponse* resp){
                if (!resp || !resp->Response) {
                    // Connection wasn't established. Should retry.
                    return ERetryErrorClass::ShortRetry;
                }

                if (resp->Response->Status == "401" ||
                    resp->Response->Status == "403" ||
                    resp->Response->Status == "413")
                {
                    return ERetryErrorClass::NoRetry;
                }

                return ERetryErrorClass::ShortRetry;
            },
            TDuration::MilliSeconds(10),
            TDuration::MilliSeconds(200),
            TDuration::Seconds(30),
            10
        );
    }

    class TMetricsPusher : public TActorBootstrapped<TMetricsPusher> {
    public:
        static constexpr char ActorName[] = "METRICS_PUSHER";

        TMetricsPusher(const NMonitoring::TDynamicCounterPtr& counters,
                       const TSolomonAgentConfig& solomonAgentConfig,
                       bool pidLabel)
                : TActorBootstrapped<TMetricsPusher>()
                , Counters(counters)
                , Config(solomonAgentConfig)
                , CommonLabels(Config.CommonLabels)
                , Url(MakeUrl())
                , MetricsSerializer(CreateMetricSerializer(Config))
                , DieOnNextPush(false)
        {
            if (pidLabel) {
                CommonLabels.emplace_back("pid", IntToString<10>(getpid()));
            }
            if (!Config.Host.empty()) {
                CommonLabels.emplace_back("host", Config.Host);
            }
            CommonLabels.emplace_back("metrics_version", TStringBuilder{} << Config.MetricsVersion);
        }

        void Bootstrap() {
            HttpProxyId = Register(NHttp::CreateHttpProxy(NMonitoring::TMetricRegistry::SharedInstance()));
            Become(&TThis::Handler, Config.UpdatePeriod, new TEvents::TEvWakeup());
        }

    protected:
        void PassAway() override {
            Send(HttpProxyId, new NActors::TEvents::TEvPoison());
            TActor::PassAway();
        }

    private:
        STRICT_STFUNC(Handler,
        {
            cFunc(TEvents::TSystem::Wakeup, Handle);
            hFunc(TEvents::TEvPoisonPill, Handle);
            hFunc(NDq::TEvHttpBase::TEvSendResult, Handle);
        });

        void Handle(TEvents::TEvPoisonPill::TPtr&) {
            DieOnNextPush = true;
        }

        void Handle(NDq::TEvHttpBase::TEvSendResult::TPtr& ev) {
            TString error = ev->Get()->HttpIncomingResponse->Get()->GetError();
            if (ev->Get()->IsTerminal) {
                Inflight.erase(ev->Cookie);
            }
            if (!error.empty()) {
                YQL_CLOG(ERROR, ProviderDq) << "Pushing metrics to solomon: Server response: " << error << "\n";
            }
            if (DieOnNextPush) {
                PassAway();
            }
        }

        void Handle() {
            Push();
            auto timeUntilNextUpdate = Config.UpdatePeriod.MicroSeconds() -
                                       Now().MicroSeconds() % Config.UpdatePeriod.MicroSeconds();
            auto duration = TDuration::MicroSeconds(timeUntilNextUpdate) + Config.UpdatePeriod / 4;
            Schedule(duration, new TEvents::TEvWakeup());
        }

        void Push() {
            if (Inflight.size() >= MaxInflight) {
                YQL_CLOG(DEBUG, ProviderDq) << "Too big inflight";
                return;
            }

            YQL_CLOG(DEBUG, ProviderDq) << "Collecting sensors";
            MetricsSerializer->WriteCommonLabels(CommonLabels);
            MetricsSerializer->Consume(Counters);
            const TString serializedMetrics = MetricsSerializer->Finish();
            auto cookie = Cookie++;
            const NHttp::THttpOutgoingRequestPtr httpRequest = BuildSolomonRequest(serializedMetrics);
            const TActorId httpSenderId = Register(NDq::CreateHttpSenderActor(SelfId(), HttpProxyId, RetryPolicy));
            Send(httpSenderId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest, TDuration::Seconds(10)), 0, cookie);
            Inflight.insert(cookie);
        }

        NHttp::THttpOutgoingRequestPtr BuildSolomonRequest(const TString& data) const {
            NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Url);
            httpRequest->Set("Authorization", Config.Authorization);
            httpRequest->Set<&NHttp::THttpRequest::ContentType>(NMonitoring::NFormatContenType::SPACK);
            httpRequest->Set<&NHttp::THttpRequest::Body>(data);
            return httpRequest;
        }

        TString MakeUrl() const {
            TStringBuilder sb;
            sb  << (!Config.Server.StartsWith("http") ? "http://" : "")
                << Config.Server
                << ":"
                << Config.Port
                << Config.Path
                << "?project=" << Config.Project
                << "&service=" << Config.Service
                << "&cluster=" << Config.Cluster;
            YQL_CLOG(DEBUG, ProviderDq) << "Url: " << sb;
            return sb;
        }

        NMonitoring::TDynamicCounterPtr Counters;
        const TSolomonAgentConfig Config;
        TSolomonAgentClient::TLabelList CommonLabels;
        const TString Url;
        std::unique_ptr<IMetricsSerializer> MetricsSerializer;
        TActorId HttpProxyId;
        bool DieOnNextPush;
        ui64 Cookie = 0;
        THashSet<ui64> Inflight;
        ui64 MaxInflight = 2;
    };

    NActors::IActor* CreateMetricsPusher(
        const NMonitoring::TDynamicCounterPtr& counters,
        const TSolomonAgentConfig& config,
        bool pidLabel)
    {
        return new TMetricsPusher(counters, config, pidLabel);
    }
} // namespace NYql
