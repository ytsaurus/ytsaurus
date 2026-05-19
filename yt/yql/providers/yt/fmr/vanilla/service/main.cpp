#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/init.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/server/yql_yt_coordinator_server.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/impl/yql_yt_coordinator_service_impl.h>
#include <yt/yql/providers/yt/fmr/gc_service/impl/yql_yt_gc_service_impl.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>
#include <yt/yql/providers/yt/fmr/job_launcher/yql_yt_job_launcher.h>
#include <yt/yql/providers/yt/fmr/job_preparer/interface/yql_yt_job_preparer_interface.h>
#include <yt/yql/providers/yt/lib/yt_download/yt_download.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/impl/yql_yt_table_data_service_local.h>
#include <yt/yql/providers/yt/fmr/table_data_service/server/yql_yt_table_data_service_server.h>
#include <yt/yql/providers/yt/fmr/vanilla/coordinator_client/yql_yt_vanilla_coordinator_client.h>
#include <yt/yql/providers/yt/fmr/vanilla/peer_tracker/yql_yt_vanilla_peer_tracker.h>
#include <yt/yql/providers/yt/fmr/vanilla/http_mon/yql_yt_vanilla_http_mon.h>
#include <yt/yql/providers/yt/fmr/vanilla/tds_discovery/yql_yt_vanilla_tds_discovery.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/impl/yql_yt_job_service_impl.h>

#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/core/file_storage/proto/file_storage.pb.h>
#include <yql/essentials/utils/backtrace/backtrace.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/log_component.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/folder/dirut.h>
#include <util/generic/guid.h>
#include <util/generic/size_literals.h>
#include <util/system/env.h>
#include <util/system/fs.h>
#include <util/system/mlock.h>

using namespace NYT;
using namespace NLastGetopt;
using namespace NYql::NFmr;

////////////////////////////////////////////////////////////////////////////////

// Job preparer for vanilla mode. Downloads the FMR job binary from a Cypress
// path on construction. UDF / remote-file downloads are not supported.
class TVanillaJobPreparer : public IFmrJobPreparer {
public:
    TVanillaJobPreparer(
        NYql::TFileStoragePtr fileStorage,
        const TString& cluster,
        const TString& fmrJobCypressPath)
        : FileStorage_(std::move(fileStorage))
    {
        if (!fmrJobCypressPath.empty()) {
            TString url = "yt://" + cluster + "?path=" + fmrJobCypressPath;
            BinaryDownload_ = FileStorage_->PutUrlAsync(url, TString());
        }
    }

    // Blocks until the binary is downloaded and returns its local path.
    // Returns empty string if no binary was configured.
    TString GetLocalFmrJobBinaryPath() {
        if (!BinaryDownload_.Initialized()) {
            return TString();
        }
        auto link = BinaryDownload_.GetValue();
        TString localPath = link->GetPath();
        NFs::SetExecutable(localPath, true);
        return localPath;
    }

    TString GenerateJobEnvironmentDir(const TString& taskId) override {
        TString dir = FileStorage_->GetTemp() / taskId;
        MakeDirIfNotExist(dir);
        return dir;
    }

    void InitalizeDistributedCache(const TString&, const TString&) override {
    }

    NThreading::TFuture<NYql::TFileLinkPtr> DownloadFileFromDistributedCache(const TString&) override {
        throw yexception() << "Distributed cache is not supported in vanilla mode";
    }

    NThreading::TFuture<NYql::TFileLinkPtr> DownloadYtResource(
        const NYT::TRichYPath&, const TString&, const TString&) override
    {
        throw yexception() << "YT resource downloads are not supported in vanilla mode";
    }

    NThreading::TFuture<NYql::TFileLinkPtr> DownloadFmrResource(
        const TFmrResourceTaskInfo&) override
    {
        throw yexception() << "FMR resource downloads are not supported in vanilla mode";
    }

private:
    NYql::TFileStoragePtr FileStorage_;
    NThreading::TFuture<NYql::TFileLinkPtr> BinaryDownload_;
};

////////////////////////////////////////////////////////////////////////////////

class TVanillaServiceJob : public IVanillaJob<> {
public:
    TVanillaServiceJob() = default;

    TVanillaServiceJob(TString cluster, ui64 jobCount, TString fmrJobBinaryPath, int verbosity)
        : Cluster_(std::move(cluster))
        , JobCount_(jobCount)
        , FmrJobBinaryPath_(std::move(fmrJobBinaryPath))
        , Verbosity_(verbosity)
    {
        YQL_ENSURE(JobCount_ > 1);
    }

    void Do() override {
        NYql::NLog::YqlLoggerScope logger(&Cerr, NYql::NLog::LegacyFormat);
        {
            auto level = NYql::NLog::TLevelHelpers::FromInt(Verbosity_);
            NYql::NLog::TComponentHelpers::ForEach([level](NYql::NLog::EComponent c) {
                NYql::NLog::YqlLogger().SetComponentLevel(c, level);
            });
        }
        NYql::NLog::YqlLogger().SetComponentLevel(
            NYql::NLog::EComponent::FastMapReduce, NYql::NLog::ELevel::TRACE);

        TVanillaPeerTracker tracker(TVanillaPeerTrackerSettings{
            .Cluster = Cluster_,
            .JobCount = JobCount_,
        });

        const ui64 selfIndex = tracker.GetSelfIndex();
        const TString selfIp = tracker.GetSelfIpAddress();
        const TString operationId = tracker.GetOperationId();

        // Vanilla TDS discovery: resolves per-cookie IPs at port 8002.
        // Passed directly into RunJob — no temp file needed.
        auto tdsDiscovery = MakeVanillaTdsDiscovery(tracker, TVanillaTdsDiscoverySettings{
            .TdsPort = 8002,
            .MinIndex = 1
        });
        tdsDiscovery->Start();

        auto tableDataServiceClient = MakeTableDataServiceClient(tdsDiscovery);

        // TDS server on non-coordinator node
        IFmrServer::TPtr tdsServer;
        if (selfIndex > 0) {
            auto localTds = MakeLocalTableDataService();
            tdsServer = MakeTableDataServiceServer(
                localTds,
                TTableDataServiceServerSettings{.Host = selfIp, .Port = 8002});
            tdsServer->Start();
        }

        // Peer HTTP server on every job node: lists peers at GET / and identifies self at GET /<cookie>.
        auto httpMon = MakeVanillaHttpMon(
            &tracker,
            TVanillaHttpMonSettings{.Host = selfIp, .Port = 8003});
        httpMon->Start();

        // Coordinator server on cookie=0 only.
        IFmrServer::TPtr coordServer;
        if (selfIndex == 0) {
            auto gcService = MakeGcService(tableDataServiceClient);
            auto coordinator = MakeFmrCoordinator(
                [&] { TFmrCoordinatorSettings s; s.WorkersNum = static_cast<ui32>(JobCount_) - 1; return s; }(),
                MakeYtCoordinatorService(),
                gcService);
            coordServer = MakeFmrCoordinatorServer(
                coordinator,
                TFmrCoordinatorServerSettings{.Port = 8001, .Host = selfIp});
            coordServer->Start();
        }

        IFmrJobFactory::TPtr jobFactory;
        IFmrWorker::TPtr worker;
        if (selfIndex > 0) {
            // Vanilla coordinator client: routes to cookie=0 at port 8001.
            auto coordClient = MakeVanillaFmrCoordinatorClient(tracker, TVanillaFmrCoordinatorClientSettings{
                .CoordinatorPort = 8001,
            });

            NYql::TFileStorageConfig fsConfig;
            fsConfig.SetThreads(2);
            fsConfig.SetMaxFiles(100);
            auto ytDownloader = NYql::MakeYtDownloader(fsConfig, Cluster_);
            auto fileStorage = NYql::CreateAsyncFileStorage(fsConfig, {ytDownloader});

            auto jobPreparer = MakeIntrusive<TVanillaJobPreparer>(
                fileStorage, Cluster_, FmrJobBinaryPath_);
            TString localBinaryPath = jobPreparer->GetLocalFmrJobBinaryPath();

            auto ytJobService = MakeYtJobSerivce();
            auto jobLauncher = MakeIntrusive<TFmrUserJobLauncher>(TFmrUserJobLauncherOptions{
                .RunInSeparateProcess = !localBinaryPath.empty(),
                .FmrJobBinaryPath = localBinaryPath,
                .GatewayType = "native",
            });

            auto jobFactorySettings = GetDefaultJobFactorySettings();
            TVanillaInfo vanillaInfo{
                .Tracker = {
                    .OperationId = operationId,
                    .SelfIndex = selfIndex,
                    .SelfJobId = tracker.GetSelfJobId(),
                    .PeerIps = tracker.GetPeerAddresses()
                }
            };
            jobFactorySettings.Function =
                [tdsDiscovery, jobLauncher, ytJobService, vanillaInfo](TTask::TPtr task, std::shared_ptr<std::atomic<bool>> cancelFlag) {
                    return RunJob(task, tdsDiscovery, vanillaInfo, ytJobService, jobLauncher, cancelFlag);
                };
            jobFactory = MakeFmrJobFactory(jobFactorySettings);
            jobFactory->Start();

            worker = MakeFmrWorker(
                coordClient,
                jobFactory,
                jobPreparer,
                TFmrWorkerSettings{.WorkerId = static_cast<ui32>(selfIndex) - 1});
            worker->Start();
        }

        // Block until this job is superseded or the operation ends.
        tracker.Run();

        if (worker) {
            worker->Stop();
        }

        if (jobFactory) {
            jobFactory->Stop();
        }

        tdsDiscovery->Stop();
        httpMon->Stop();
        if (tdsServer) {
            tdsServer->Stop();
        }

        if (coordServer) {
            coordServer->Stop();
        }
    }

    Y_SAVELOAD_JOB(Cluster_, JobCount_, FmrJobBinaryPath_, Verbosity_);

private:
    TString Cluster_;
    ui64 JobCount_ = 1;
    TString FmrJobBinaryPath_;
    int Verbosity_ = static_cast<int>(TLOG_ERR);
};

REGISTER_VANILLA_JOB(TVanillaServiceJob);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char* argv[]) {
    try {
        LockAllMemory(LockCurrentMemory | LockFutureMemory);
    } catch (yexception&) {
        Cerr << "mlockall failed, but that's fine" << Endl;
    }

    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();

    Initialize(argc, argv);

    TOpts opts;

    TString cluster;
    TString pool;
    TString networkProject;
    TString fmrJobBinaryPath;
    TString alias;
    ui64 jobCount = 0;
    int verbosity = static_cast<int>(TLOG_ERR);

    opts.AddLongOption("cluster", "YT cluster URL (e.g. hahn)").Required().StoreResult(&cluster);
    opts.AddLongOption("pool", "YT pool to run in").Optional().StoreResult(&pool);
    opts.AddLongOption("alias", "Operation alias").Required().StoreResult(&alias);
    opts.AddLongOption("network-project", "Network project name").Required().StoreResult(&networkProject);
    opts.AddLongOption("job-count", "Number of service jobs (>1)").Required().StoreResult(&jobCount);
    opts.AddLongOption("fmr-job-bin", "Cypress path to the FMR job binary").Optional().StoreResult(&fmrJobBinaryPath);
    opts.AddLongOption('v', "verbosity", "Logging verbosity level").Optional().StoreResult(&verbosity).DefaultValue(verbosity);

    TOptsParseResult parseResult(&opts, argc, argv);

    YQL_ENSURE(jobCount > 1);

    NYql::NLog::YqlLoggerScope logger(&Cerr, NYql::NLog::LegacyFormat);
    {
        auto level = NYql::NLog::TLevelHelpers::FromInt(verbosity);
        NYql::NLog::TComponentHelpers::ForEach([level](NYql::NLog::EComponent c) {
            NYql::NLog::YqlLogger().SetComponentLevel(c, level);
        });
        NYql::NLog::YqlLogger().SetComponentLevel(
            NYql::NLog::EComponent::FastMapReduce, NYql::NLog::ELevel::TRACE);
    }

    auto client = CreateClient(cluster);

    auto operationSpec = TVanillaOperationSpec().MaxFailedJobCount(0).Alias("*" + alias);
    if (!pool.empty()) {
        operationSpec = operationSpec.Pool(pool);
    }

    constexpr size_t DefaultMemoryLimit = 4_GB;
    NYT::TUserJobSpec userJobSpec;
    userJobSpec.MemoryLimit(DefaultMemoryLimit);
    auto task = TVanillaTask()
        .Name("main")
        .JobCount(jobCount)
        .Job(new TVanillaServiceJob(cluster, jobCount, fmrJobBinaryPath, verbosity))
        .NetworkProject(networkProject)
        .Spec(userJobSpec);

    operationSpec.AddTask(task);

    TNode extraSpec;
    extraSpec["issue_temporary_token"] = true;

    auto operation = client->RunVanilla(
        operationSpec,
        TOperationOptions()
            .StartOperationMode(TOperationOptions::EStartOperationMode::SyncStart)
            .Spec(extraSpec));

    Cerr << "https://yt.yandex-team.ru/" << cluster << "/operations/"
         << GetGuidAsString(operation->GetId()) << Endl;

    return 0;
}
