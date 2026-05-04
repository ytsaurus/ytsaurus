#include "yql_yt_vanilla_peer_tracker.h"

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/http/simple/http_client.h>

#include <util/datetime/base.h>
#include <util/generic/guid.h>
#include <util/random/random.h>
#include <util/system/env.h>
#include <util/stream/str.h>

namespace NYql::NFmr {

    ////////////////////////////////////////////////////////////////////////////////

    namespace {

        constexpr int VanillaPingPort = 8000;

        bool IsBackboneMtn(const TString& ip) {
            return ip.StartsWith("2a02:6b8:c");
        }

    } // namespace

    TVanillaPeerTracker::TVanillaPeerTracker(TVanillaPeerTrackerSettings settings)
        : Settings_(std::move(settings))
        , SelfCookie_(FromString<ui64>(GetEnv("YT_JOB_COOKIE")))
        , SelfJobId_(GetEnv("YT_JOB_ID"))
        , SelfIpAddress_(GetEnv("YT_IP_ADDRESS_DEFAULT"))
        , OperationId_(GetEnv("YT_OPERATION_ID"))
    {
        Y_ENSURE(SelfCookie_ < Settings_.JobCount,
                 "Self cookie " << SelfCookie_ << " is out of range [0, " << Settings_.JobCount << ")");

        PeerIps_.resize(Settings_.JobCount);

        Cerr << "Job started " << SelfJobId_
             << " cookie=" << SelfCookie_
             << " ip=" << SelfIpAddress_
             << " at " << Now() << Endl;
    }

    TString TVanillaPeerTracker::GetOperationId() const {
        return OperationId_;
    }

    ui64 TVanillaPeerTracker::GetSelfIndex() const {
        return SelfCookie_;
    }

    TString TVanillaPeerTracker::GetSelfJobId() const {
        return SelfJobId_;
    }

    TString TVanillaPeerTracker::GetSelfIpAddress() const {
        return SelfIpAddress_;
    }

    ui64 TVanillaPeerTracker::GetPeerCount() const {
        return Settings_.JobCount;
    }

    TString TVanillaPeerTracker::GetPeerAddress(ui64 index) const {
        TGuard guard(PeersMutex_);
        Y_ENSURE(index < PeerIps_.size(), "Peer index " << index << " is out of range [0, " << PeerIps_.size() << ")");
        return PeerIps_[index];
    }

    TVector<TString> TVanillaPeerTracker::GetPeerAddresses() const {
        TGuard guard(PeersMutex_);
        return PeerIps_;
    }

    void TVanillaPeerTracker::Run() {
        using namespace NYT;

        auto client = CreateClient(Settings_.Cluster);

        Shutdown_.store(false);

        // Thread 1: HTTP ping server.
        ServerThread_ = MakeHolder<TThread>([&]() {
            struct TPingRequest: public TRequestReplier {
                bool DoReply(const TReplyParams& params) override {
                    params.Output << THttpResponse(HTTP_OK).SetContent("ok");
                    return true;
                }
            };
            struct TPingCallback: public THttpServer::ICallBack {
                TClientRequest* CreateClient() override {
                    return new TPingRequest();
                }
            };
            TPingCallback cb;
            THttpServer::TOptions opts;
            opts.AddBindAddress(SelfIpAddress_, VanillaPingPort);
            THttpServer server(&cb, opts);
            server.Start();
            while (!Shutdown_.load()) {
                Sleep(TDuration::Seconds(1));
            }
            server.Stop();
        });

        // Thread 2: periodically ping a random peer.
        ClientThread_ = MakeHolder<TThread>([&]() {
            while (!Shutdown_.load()) {
                TString ip;
                {
                    TGuard guard(PeersMutex_);
                    if (!PeerIps_.empty()) {
                        ip = PeerIps_[RandomNumber<size_t>(PeerIps_.size())];
                    }
                }
                if (!ip.empty()) {
                    try {
                        TKeepAliveHttpClient httpClient(ip, VanillaPingPort,
                                                        Settings_.PingTimeout, Settings_.PingTimeout);
                        TStringStream response;
                        httpClient.DoGet("/ping", &response);
                        Cerr << "Pinged " << ip << ": " << response.Str() << Endl;
                    } catch (...) {
                        Cerr << "Ping to " << ip << " failed: " << CurrentExceptionMessage() << Endl;
                    }
                }
                Sleep(Settings_.PingClientInterval);
            }
        });

        ServerThread_->Start();
        ClientThread_->Start();

        // Main loop: list jobs, update peer snapshot, detect supersede.
        bool running = true;
        while (running) {
            try {
                Cerr << "Listing jobs at " << Now() << Endl;

                auto result = client->ListJobs(GetGuid(OperationId_),
                                               TListJobsOptions()
                                                   .State(EJobState::Running)
                                                   .DataSource(EListJobsDataSource::Runtime)
                                                   .Limit(static_cast<i64>(Settings_.JobCount)));

                auto& jobs = result.Jobs;
                SortBy(jobs, [](const TJobAttributes& job) { return GetGuidAsString(*job.Id); });

                TVector<TString> newPeerIps(Settings_.JobCount);
                for (const auto& job : jobs) {
                    TString jobId = GetGuidAsString(*job.Id);

                    if (job.Cookie) {
                        Y_ENSURE(*job.Cookie < Settings_.JobCount,
                                 "Peer cookie " << *job.Cookie << " is out of range [0, " << Settings_.JobCount << ")");
                    }

                    if (job.Cookie && *job.Cookie == SelfCookie_ && jobId != SelfJobId_) {
                        Cerr << "Superseded by job " << jobId << ", exiting" << Endl;
                        running = false;
                    }

                    TString ip;
                    if (job.ExecAttributes) {
                        try {
                            for (const auto& node : (*job.ExecAttributes)["ip_addresses"].AsList()) {
                                const auto& candidate = node.AsString();
                                // backbone MTN prefix
                                if (IsBackboneMtn(candidate)) {
                                    ip = candidate;
                                    break;
                                }
                            }
                        } catch (...) {
                        }
                    }

                    Cerr << "Job " << jobId
                         << " cookie=" << job.Cookie
                         << " ip=" << (ip.empty() ? "<unknown>" : ip) << Endl;

                    if (job.Cookie) {
                        newPeerIps[*job.Cookie] = ip;
                    }
                }

                {
                    TGuard guard(PeersMutex_);
                    PeerIps_ = std::move(newPeerIps);
                }
            } catch (...) {
                Cerr << "Error: " << CurrentExceptionMessage() << Endl;
            }

            if (running) {
                Sleep(Settings_.ListJobsInterval);
            }
        }

        Shutdown_.store(true);
        ServerThread_->Join();
        ClientThread_->Join();
    }

    void TVanillaPeerTracker::CheckOperation(
        const TString& cluster,
        const TString& operationId,
        bool withPing,
        TDuration pingTimeout)
    {
        using namespace NYT;

        auto client = CreateClient(cluster);

        auto result = client->ListJobs(
            GetGuid(operationId),
            TListJobsOptions()
                .State(EJobState::Running)
                .DataSource(EListJobsDataSource::Runtime));

        const TJobAttributes* cookie0Job = nullptr;
        for (const auto& job : result.Jobs) {
            if (job.Cookie && *job.Cookie == 0) {
                cookie0Job = &job;
                break;
            }
        }

        if (!cookie0Job) {
            Cerr << "No running job with cookie 0 found in operation " << operationId << Endl;
            return;
        }

        TString ip;
        if (cookie0Job->ExecAttributes) {
            try {
                for (const auto& node : (*cookie0Job->ExecAttributes)["ip_addresses"].AsList()) {
                    const auto& candidate = node.AsString();
                    if (IsBackboneMtn(candidate)) {
                        ip = candidate;
                        break;
                    }
                }
            } catch (...) {
            }
        }

        Cerr << "Job with cookie=0"
             << " id=" << GetGuidAsString(*cookie0Job->Id)
             << " ip=" << (ip.empty() ? "<unknown>" : ip) << Endl;

        if (!withPing) {
            return;
        }

        if (ip.empty()) {
            Cerr << "Cannot ping: IP address is unknown" << Endl;
            return;
        }

        try {
            TKeepAliveHttpClient httpClient(ip, VanillaPingPort, pingTimeout, pingTimeout);
            TStringStream response;
            httpClient.DoGet("/ping", &response);
            Cerr << "Ping to " << ip << " succeeded: " << response.Str() << Endl;
        } catch (...) {
            Cerr << "Ping to " << ip << " failed: " << CurrentExceptionMessage() << Endl;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

} // namespace NYql::NFmr
