#include "service_node_pinger.h"
#include <yt/yql/providers/dq/testlib/test_coordination_helper.h>

#include <yt/yql/providers/dq/actors/yt/yt_wrapper.h>
#include <yt/yql/providers/dq/runtime/file_cache.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>
#include <contrib/ydb/library/yql/providers/dq/runtime/runtime_data.h>

#include <contrib/libs/grpc/include/grpcpp/server_builder.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/folder/tempdir.h>
#include <util/generic/hash_set.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

namespace NYql {
    namespace {

        class TDownloadListService: public Yql::DqsProto::DqService::Service {
        public:
            grpc::Status RegisterNode(grpc::ServerContext*, const Yql::DqsProto::RegisterNodeRequest* request,
                                      Yql::DqsProto::RegisterNodeResponse* response) override {
                Request.TrySetValue(*request);
                *response = Response;
                return grpc::Status::OK;
            }

            Yql::DqsProto::RegisterNodeResponse Response;
            NThreading::TPromise<Yql::DqsProto::RegisterNodeRequest> Request =
                NThreading::NewPromise<Yql::DqsProto::RegisterNodeRequest>();
        };

        class TPingerCoordinator: public TTestCoordinationHelper {
        public:
            TPingerCoordinator(ui32 nodeId, THashMap<TString, NActors::TActorId> wrappers)
                : TTestCoordinationHelper(nodeId)
                , Wrappers(std::move(wrappers))
            {
                Config.SetHeartbeatPeriodMs(3'600'000);
            }

            NActors::TActorId GetWrapper(NActors::TActorSystem*, const TString& cluster,
                                         const TString&, const TString&, const TString& = {}) override {
                return Wrappers.at(cluster);
            }

            TWorkerRuntimeData* GetRuntimeData() override {
                return &RuntimeData;
            }

            const NProto::TDqConfig::TYtCoordinator& GetConfig() override {
                return Config;
            }

        private:
            const THashMap<TString, NActors::TActorId> Wrappers;
            TWorkerRuntimeData RuntimeData;
            NProto::TDqConfig::TYtCoordinator Config;
        };

        using TExpectedReads = THashMap<TString, THashMap<TString, TString>>;

        void CheckMonitoringPort(TMaybe<ui16> monitoringPort) {
            using namespace NActors;
            TDownloadListService service;
            int port = 0;
            grpc::ServerBuilder builder;
            builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &port);
            builder.RegisterService(&service);
            auto server = builder.BuildAndStart();
            UNIT_ASSERT(server);
            UNIT_ASSERT(port);

            TTestActorRuntimeBase runtime(1, true);
            runtime.Initialize();
            auto coordinator = MakeIntrusive<TPingerCoordinator>(
                runtime.GetNodeId(0), THashMap<TString, TActorId>{});
            TResourceManagerOptions options;
            if (monitoringPort) {
                options.MonitoringPort = *monitoringPort;
            }
            auto resolver = CreateStaticResolver({TStringBuilder() << "localhost:" << port});
            runtime.Register(CreateServiceNodePinger(runtime.GetNodeId(0), "localhost", 12345,
                                                     "worker_node", {}, resolver, coordinator, options));

            const auto request = service.Request.GetFuture();
            UNIT_ASSERT_C(request.Wait(TDuration::Seconds(10)), "Worker registration timed out");
            UNIT_ASSERT_VALUES_EQUAL(request.GetValueSync().GetMonitoringPort(), monitoringPort.GetOrElse(0));
            UNIT_ASSERT_VALUES_EQUAL(request.GetValueSync().GetPort(), 12345);
            server->Shutdown();
        }

        void CheckDownloads(const Yql::DqsProto::RegisterNodeResponse& response, TExpectedReads expectedReads) {
            using namespace NActors;
            TTempDir tempDir;
            TDownloadListService service;
            service.Response = response;
            THashSet<TString> expectedIds;
            for (const auto& [cluster, reads] : expectedReads) {
                for (const auto& [path, id] : reads) {
                    expectedIds.insert(id);
                }
            }
            int port = 0;
            grpc::ServerBuilder builder;
            builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &port);
            builder.RegisterService(&service);
            auto server = builder.BuildAndStart();
            UNIT_ASSERT(server);
            UNIT_ASSERT(port);

            TTestActorRuntimeBase runtime(1, true);
            runtime.Initialize();
            THashMap<TString, TActorId> wrappers;
            for (const auto& [cluster, _] : expectedReads) {
                wrappers.emplace(cluster, runtime.AllocateEdgeActor());
            }
            auto coordinator = MakeIntrusive<TPingerCoordinator>(runtime.GetNodeId(0), wrappers);
            TResourceManagerOptions options;
            options.YtBackend.SetProxyAddress("worker-cluster.yt");
            options.YtBackend.SetUploadPrefix("//worker");
            options.YtBackend.SetUser("worker-user");
            options.TmpDir = tempDir.Name() + "/download";
            options.FileCache = MakeIntrusive<TFileCache>(tempDir.Name() + "/cache", 1024);
            auto resolver = CreateStaticResolver({TStringBuilder() << "localhost:" << port});
            runtime.Register(CreateServiceNodePinger(runtime.GetNodeId(0), "localhost", 12345,
                                                     "worker_node", {}, resolver, coordinator, options));

            for (auto& [cluster, reads] : expectedReads) {
                const auto wrapper = wrappers.at(cluster);
                while (!reads.empty()) {
                    const auto read = runtime.GrabEdgeEvent<TEvReadFile>(wrapper, TDuration::Seconds(10));
                    UNIT_ASSERT(read);
                    const auto remotePath = std::get<0>(*read->Get()).GetPath();
                    const auto it = reads.find(remotePath);
                    UNIT_ASSERT_C(it != reads.end(), cluster << ": unexpected path " << remotePath);
                    const auto& localPath = std::get<1>(*read->Get());
                    TFsPath(localPath).Parent().MkDirs();
                    {
                        TFileOutput output(localPath);
                        output << it->second;
                    }
                    runtime.Send(new IEventHandle(read->Sender, wrapper,
                                                  new TEvReadFileResponse(read->Get()->RequestId, NYT::TErrorOr<void>())));
                    reads.erase(it);
                }
            }

            runtime.WaitFor("downloaded resources", [&] {
                for (const auto& id : expectedIds) {
                    const auto cached = options.FileCache->FindFile(id);
                    if (!cached || !TFsPath(*cached).Exists()) {
                        return false;
                    }
                }
                return true;
            }, TDuration::Seconds(10));
            for (const auto& id : expectedIds) {
                const auto cached = options.FileCache->FindFile(id);
                UNIT_ASSERT(cached);
                UNIT_ASSERT_VALUES_EQUAL(TFileInput(*cached).ReadAll(), id);
            }
            server->Shutdown();
        }

        void CheckGroupedDownloads() {
            Yql::DqsProto::RegisterNodeResponse response;
            const TVector<std::tuple<TString, TString, TString>> resources = {
                {"one-file", "cluster-one.yt", "//shared"},
                {"one-more-file", "cluster-one.yt", "//shared"},
                {"two-file", "cluster-two.yt", "//shared"},
                {"other-prefix-file", "cluster-one.yt", "//other"},
                {"legacy-file", "", ""},
            };
            TExpectedReads expectedReads;
            for (const auto& [id, cluster, prefix] : resources) {
                expectedReads[cluster.empty() ? "worker-cluster.yt" : cluster].emplace(
                    (prefix.empty() ? TString("//worker") : prefix) + "/udfs/" + id, id);
            }
            auto addGroup = [&] (const TString& cluster, const TString& prefix, const TVector<TString>& ids) {
                auto* group = response.AddDownloadGroups();
                group->SetUploadClusterAddress(cluster);
                group->SetUploadPrefix(prefix);
                for (const auto& id : ids) {
                    auto* file = group->AddFiles();
                    file->SetObjectId(id);
                    file->SetObjectType(Yql::DqsProto::TFile::EUDF_FILE);
                }
            };
            addGroup("cluster-one.yt", "//shared", {"one-file", "one-more-file"});
            addGroup("cluster-two.yt", "//shared", {"two-file"});
            addGroup("cluster-one.yt", "//other", {"other-prefix-file"});
            addGroup("", "", {"legacy-file"});
            addGroup("cluster-one.yt", "//empty", {});

            for (const auto& group : response.GetDownloadGroups()) {
                for (const auto& file : group.GetFiles()) {
                    *response.AddDownloadList() = file;
                }
            }
            CheckDownloads(response, expectedReads);
        }
    } // namespace

    Y_UNIT_TEST_SUITE(TServiceNodePingerTest) {
        Y_UNIT_TEST(ReportsMonitoringPort) {
            CheckMonitoringPort(31004);
        }

        Y_UNIT_TEST(ReportsDisabledMonitoringPort) {
            CheckMonitoringPort(0);
        }

        Y_UNIT_TEST(DefaultsToDisabledMonitoringPort) {
            CheckMonitoringPort({});
        }

        Y_UNIT_TEST(PrefersGroupsOverLegacyList) {
            CheckGroupedDownloads();
        }

        Y_UNIT_TEST(DownloadsLegacyListFromConfiguredLocation) {
            Yql::DqsProto::RegisterNodeResponse response;
            auto* udf = response.AddDownloadList();
            udf->SetObjectId("legacy-udf");
            udf->SetObjectType(Yql::DqsProto::TFile::EUDF_FILE);
            auto* exe = response.AddDownloadList();
            exe->SetObjectId("legacy-exe");
            exe->SetObjectType(Yql::DqsProto::TFile::EEXE_FILE);
            exe->SetName("dq_vanilla_job.lite");

            CheckDownloads(response, {{"worker-cluster.yt", {
                {"//worker/udfs/legacy-udf", "legacy-udf"},
                {"//worker/bin/legacy-exe/dq_vanilla_job.lite", "legacy-exe"},
            }}});
        }
    } // Y_UNIT_TEST_SUITE(TServiceNodePingerTest)

} // namespace NYql
