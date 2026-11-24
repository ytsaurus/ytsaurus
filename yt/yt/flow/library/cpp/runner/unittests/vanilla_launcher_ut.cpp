#include <yt/yt/flow/library/cpp/runner/config.h>
#include <yt/yt/flow/library/cpp/runner/root_clients_cache.h>
#include <yt/yt/flow/library/cpp/runner/vanilla_launcher.h>

#include <yt/yt/flow/library/cpp/controller/config.h>

#include <yt/yt/flow/library/cpp/companion/client/config.h>

#include <yt/yt/flow/library/cpp/vanilla/spec.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/not_implemented_client.h>
#include <yt/yt/client/api/options.h>

#include <yt/yt/client/api/rpc_proxy/config.h>

#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/cache/config.h>

#include <yt/yt/client/security_client/acl.h>
#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>

#include <util/folder/tempdir.h>

#include <util/generic/guid.h>

#include <util/stream/file.h>

#include <util/string/cast.h>

#include <util/system/env.h>
#include <util/system/fs.h>
#include <util/system/tempfile.h>

#include <set>

namespace NYT::NFlow {
namespace {

using namespace NClient::NCache;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TVanillaConfigTest, AllowsCustomNetworkProject)
{
    auto config = ConvertTo<TVanillaConfigPtr>(
        TYsonStringBuf(R"({pool=test;worker={count=1};network_project=custom})"));

    EXPECT_EQ(config->NetworkProject, std::optional<std::string>("custom"));
}

TEST(TVanillaConfigTest, EntityDisablesDefaultNetworkProject)
{
    auto config = ConvertTo<TVanillaConfigPtr>(
        TYsonStringBuf(R"({pool=test;worker={count=1};network_project=#})"));

    EXPECT_FALSE(config->NetworkProject.has_value());
}

TEST(TVanillaConfigTest, AllowsPerTaskDockerImage)
{
    auto config = ConvertTo<TVanillaConfigPtr>(
        TYsonStringBuf(R"({pool=test;worker={count=1;docker_image="registry.example.com/image:tag"}})"));

    EXPECT_EQ(config->Worker->DockerImage, std::optional<std::string>("registry.example.com/image:tag"));
    EXPECT_FALSE(config->Controller->DockerImage.has_value());
}

TEST(TVanillaConfigTest, ConfiguresWorkerJobProxy)
{
    auto config = ConvertTo<TVanillaConfigPtr>(TYsonStringBuf(
        R"({pool=test;worker={count=1;enable_rpc_proxy_in_job_proxy=%true}})"));
    EXPECT_TRUE(config->Worker->EnableRpcProxyInJobProxy);
    EXPECT_FALSE(config->Controller->EnableRpcProxyInJobProxy);
}

// Without a network project the jobs share the exec node's network, where the fixed ports of
// co-located flow jobs collide — the out-of-the-box launch must ask YT for ports instead.
TEST(TVanillaConfigTest, DefaultsPortCountsWithoutNetworkProject)
{
    auto config = ConvertTo<TVanillaConfigPtr>(
        TYsonStringBuf(R"({pool=test;worker={count=1};network_project=#})"));

    EXPECT_EQ(config->Controller->PortCount, std::optional<int>(2));
    EXPECT_EQ(config->Worker->PortCount, std::optional<int>(4));
}

TEST(TVanillaConfigTest, KeepsFixedPortsUnderNetworkProject)
{
    auto config = ConvertTo<TVanillaConfigPtr>(
        TYsonStringBuf(R"({pool=test;worker={count=1};network_project=custom})"));

    EXPECT_FALSE(config->Controller->PortCount.has_value());
    EXPECT_FALSE(config->Worker->PortCount.has_value());
}

TEST(TVanillaConfigTest, ExplicitZeroPortCountKeepsFixedPorts)
{
    auto config = ConvertTo<TVanillaConfigPtr>(TYsonStringBuf(
        R"({pool=test;controller={count=1;port_count=0};worker={count=1;port_count=0};network_project=#})"));

    EXPECT_EQ(config->Controller->PortCount, std::optional<int>(0));
    EXPECT_EQ(config->Worker->PortCount, std::optional<int>(0));
}

TEST(TVanillaConfigAclTest, OmittedAclStaysUnset)
{
    auto config = ConvertTo<TVanillaConfigPtr>(TYsonStringBuf(R"({pool=test;worker={count=1}})"));

    EXPECT_FALSE(config->Acl);
}

TEST(TVanillaConfigAclTest, EntityAclStaysUnset)
{
    auto config = ConvertTo<TVanillaConfigPtr>(TYsonStringBuf(R"({pool=test;worker={count=1};acl=#})"));

    EXPECT_FALSE(config->Acl);
}

TEST(TVanillaConfigAclTest, EmptyAclStaysPresent)
{
    auto config = ConvertTo<TVanillaConfigPtr>(TYsonStringBuf(R"({pool=test;worker={count=1};acl=[]})"));

    ASSERT_TRUE(config->Acl);
    EXPECT_TRUE(config->Acl->Entries.empty());
}

TEST(TVanillaConfigAclTest, KeepsExplicitAcl)
{
    auto config = ConvertTo<TVanillaConfigPtr>(TYsonStringBuf(R"({
        pool=test;
        worker={count=1};
        acl=[
            {action=allow;subjects=["pipeline-operators"];permissions=[read;manage];};
            {action=deny;subjects=[blocked];permissions=[read];subject_tag_filter="untrusted"};
        ];
    })"));

    NSecurityClient::TSerializableAccessControlList expectedAcl;
    expectedAcl.Entries.emplace_back(
        NSecurityClient::ESecurityAction::Allow,
        std::vector<std::string>{"pipeline-operators"},
        EPermission::Read | EPermission::Manage);
    expectedAcl.Entries.emplace_back(
        NSecurityClient::ESecurityAction::Deny,
        std::vector<std::string>{"blocked"},
        EPermission::Read);
    expectedAcl.Entries.back().SubjectTagFilter = MakeBooleanFormula("untrusted");

    ASSERT_TRUE(config->Acl);
    EXPECT_EQ(*config->Acl, expectedAcl);
}

TEST(TVanillaConfigAclTest, RejectsMalformedAcl)
{
    EXPECT_ANY_THROW(ConvertTo<TVanillaConfigPtr>(
        TYsonStringBuf(R"({pool=test;worker={count=1};acl=[{action=allow;permissions=[read]}]})")));
}

TEST(TVanillaConfigAclTest, RejectsNonOperationPermissions)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ConvertTo<TVanillaConfigPtr>(TYsonStringBuf(
            R"({pool=test;worker={count=1};acl=[{action=allow;subjects=[writers];permissions=[read;write]}]})")),
        "permissions are allowed in operation ACL");
}

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf PipelineCluster = "pipeline-cluster";
// Stands for the address a runner pins when the cluster advertises an RPC proxy it cannot reach.
constexpr TStringBuf PinnedProxyAddress = "[64:ff9b::1]:9013";

NYPath::TRichYPath MakePipelinePath()
{
    NYPath::TRichYPath path("//tmp/pipeline");
    path.SetCluster(TString(PipelineCluster));
    return path;
}

TVanillaConfigPtr MakeVanillaConfig(TStringBuf extra = {})
{
    auto yson = Format("{enable=%%true; pool=test; worker={count=1}; %v}", extra);
    return ConvertTo<TVanillaConfigPtr>(TYsonStringBuf(yson));
}

//! The clients cache a runner ends up with when its config pins the proxy address and turns
//! discovery off — the shape that lets it reach a cluster advertising an unreachable address.
IClientsCachePtr MakeCacheWithPinnedProxy()
{
    auto config = New<TClientsCacheConfig>();
    config->SetDefaults();
    config->DefaultConnection->EnableProxyDiscovery = false;
    config->DefaultConnection->ProxyAddresses = std::vector<std::string>{std::string(PinnedProxyAddress)};

    return CreateRootClientsCache({
        .PipelinePath = MakePipelinePath(),
        .ClientsCacheConfig = std::move(config),
        .ClientOptions = NApi::TClientOptions::FromUser("test-user"),
    });
}

constexpr TStringBuf StopMarker = "stopped by the test at cluster";

//! Asserts that the launcher asks for |expectedCluster|: that one is served from |underlying|,
//! any other stops the launch and names itself in the error. Stopping is also what keeps the
//! launcher from issuing a request, so these tests need no cluster to run against.
class TAssertClientsCache
    : public IClientsCache
{
public:
    TAssertClientsCache(TStringBuf expectedCluster, IClientsCachePtr underlying)
        : ExpectedCluster_(expectedCluster)
        , Underlying_(std::move(underlying))
    { }

    NApi::IClientPtr GetClient(TStringBuf clusterUrl) override
    {
        THROW_ERROR_EXCEPTION_IF(clusterUrl != ExpectedCluster_, "%v %Qv", StopMarker, clusterUrl);

        return Underlying_->GetClient(clusterUrl);
    }

private:
    const std::string ExpectedCluster_;
    const IClientsCachePtr Underlying_;
};

DEFINE_REFCOUNTED_TYPE(TAssertClientsCache)

////////////////////////////////////////////////////////////////////////////////

// Fixed-port workers configure companion RPC and monitoring ports.
TEST(TVanillaNodeConfigTest, CarriesCompanionPort)
{
    auto nodeConfig = BuildDefaultVanillaNodeConfig(
        MakePipelinePath(),
        /*proxyRole*/ std::nullopt,
        /*workerPortCount*/ std::nullopt);

    ASSERT_TRUE(nodeConfig->Companion);
    EXPECT_GT(nodeConfig->Companion->Port, 0);
    EXPECT_GT(nodeConfig->Companion->MonitoringPort, 0);
    EXPECT_NE(nodeConfig->Companion->Port, nodeConfig->Companion->MonitoringPort);
}

// YT-allocated workers receive companion ports from YT_PORT_2 and YT_PORT_3.
TEST(TVanillaNodeConfigTest, OmitsCompanionPortForYtAllocatedPorts)
{
    auto nodeConfig = BuildDefaultVanillaNodeConfig(
        MakePipelinePath(),
        /*proxyRole*/ std::nullopt,
        /*workerPortCount*/ 2);

    EXPECT_FALSE(nodeConfig->Companion);
}

// The sandbox is discarded with the operation, so the job stderr — the one artifact YT retains
// for finished jobs — must carry the info-level pipeline history, not just crash traces.
TEST(TVanillaNodeConfigTest, MirrorsInfoLogToStderr)
{
    auto nodeConfig = BuildDefaultVanillaNodeConfig(
        MakePipelinePath(),
        /*proxyRole*/ std::nullopt,
        /*workerPortCount*/ std::nullopt);
    auto loggingConfig = nodeConfig->GetSingletonConfig<NLogging::TLogManagerConfig>();

    auto writersFor = [&] (TStringBuf category, NLogging::ELogLevel level) {
        std::set<std::string> writers;
        for (const auto& rule : loggingConfig->Rules) {
            if (rule->IsApplicable(category, level, NLogging::ELogFamily::PlainText)) {
                writers.insert(rule->Writers.begin(), rule->Writers.end());
            }
        }
        return writers;
    };

    EXPECT_EQ(writersFor("Worker", NLogging::ELogLevel::Info), (std::set<std::string>{"file", "stderr"}));
    // Chatty infrastructure categories: the info-level stream is dropped, errors still reach stderr.
    EXPECT_EQ(writersFor("Bus", NLogging::ELogLevel::Info), std::set<std::string>{});
    EXPECT_EQ(writersFor("Bus", NLogging::ELogLevel::Error), std::set<std::string>{"stderr"});
}

////////////////////////////////////////////////////////////////////////////////

// Ten, the scheduler default, would drop most jobs' stderrs; the runner defaults to the scheduler
// cap so every job's stderr survives the operation.
TEST(TVanillaConfigTest, RetainsStderrOfEveryJobByDefault)
{
    EXPECT_EQ(MakeVanillaConfig()->MaxStderrCount, DefaultMaxStderrCount);
    EXPECT_EQ(MakeVanillaConfig("max_stderr_count=7")->MaxStderrCount, 7);
}

TEST(TVanillaSpecTest, CarriesCpuLimitToJobEnvironment)
{
    TVanillaSpec spec;
    spec.Tasks.push_back(TVanillaTaskSpec{
        .Name = "worker",
        .FlowMode = "worker",
        .CpuLimit = 10,
    });
    auto operation = BuildVanillaOperationSpec(spec);
    auto worker = operation->GetChildOrThrow("tasks")->AsMap()->GetChildOrThrow("worker")->AsMap();
    auto environment = ConvertTo<THashMap<std::string, std::string>>(worker->GetChildOrThrow("environment"));

    EXPECT_DOUBLE_EQ(FromString<double>(environment.at("YT_FLOW_CPU_LIMIT")), 10);
    EXPECT_EQ(ConvertTo<double>(worker->GetChildOrThrow("cpu_limit")), 10);
    EXPECT_FALSE(worker->FindChild("set_container_cpu_limit"));
}

TEST(TVanillaSpecTest, EnforcesContainerCpuLimitOnRequest)
{
    TVanillaSpec spec;
    spec.Tasks.push_back(TVanillaTaskSpec{
        .Name = "worker",
        .FlowMode = "worker",
        .CpuLimit = 10,
        .SetContainerCpuLimit = true,
    });
    auto operation = BuildVanillaOperationSpec(spec);
    auto worker = operation->GetChildOrThrow("tasks")->AsMap()->GetChildOrThrow("worker")->AsMap();
    EXPECT_TRUE(ConvertTo<bool>(worker->GetChildOrThrow("set_container_cpu_limit")));
}

TEST(TVanillaSpecTest, RequestsCpuLimitOnlyForSelectedTasks)
{
    TVanillaSpec spec;
    spec.Tasks = {
        TVanillaTaskSpec{
            .Name = "controller",
            .CpuLimit = 1,
        },
        TVanillaTaskSpec{
            .Name = "worker",
            .CpuLimit = 2,
            .SetContainerCpuLimit = true,
        },
    };
    auto tasks = BuildVanillaOperationSpec(spec)->GetChildOrThrow("tasks")->AsMap();
    auto controller = tasks->GetChildOrThrow("controller")->AsMap();
    auto worker = tasks->GetChildOrThrow("worker")->AsMap();

    EXPECT_FALSE(controller->FindChild("set_container_cpu_limit"));
    EXPECT_TRUE(ConvertTo<bool>(worker->GetChildOrThrow("set_container_cpu_limit")));
    EXPECT_EQ(ConvertTo<double>(worker->GetChildOrThrow("cpu_limit")), 2);
}

TEST(TVanillaSpecTest, CarriesMaxStderrCount)
{
    auto spec = BuildVanillaOperationSpec(TVanillaSpec{.MaxStderrCount = 7});

    EXPECT_EQ(ConvertTo<int>(spec->GetChildOrThrow("max_stderr_count")), 7);
}

////////////////////////////////////////////////////////////////////////////////

std::string WriteTestFile(const TTempDir& dir, const std::string& fileName)
{
    auto path = std::string(dir.Name()) + "/" + fileName;
    TFileOutput(TString(path)).Write("test");
    return path;
}

std::vector<std::pair<std::string, std::string>> ToFileNamesAndPaths(const std::vector<NDetail::TJobSharedLibrary>& libraries)
{
    std::vector<std::pair<std::string, std::string>> result;
    for (const auto& library : libraries) {
        result.emplace_back(library.FileName, library.LocalPath);
    }
    return result;
}

TEST(TJobSharedLibrariesTest, SkipsMainProgramVdsoAndSystemLibraries)
{
    TTempDir dir;
    auto idn = WriteTestFile(dir, "liblibidn-dynamic.so");
    auto iconv = WriteTestFile(dir, "liblibiconv-dynamic.so");

    auto libraries = NDetail::SelectJobSharedLibraries({
        "",
        "linux-vdso.so.1",
        "linux-gate.so.1",
        idn,
        "/lib64/ld-linux-x86-64.so.2",
        "/lib/x86_64-linux-gnu/libc.so.6",
        "/usr/lib/x86_64-linux-gnu/libstdc++.so.6",
        "/usr/lib64/libm.so.6",
        iconv,
    });

    EXPECT_EQ(ToFileNamesAndPaths(libraries), (std::vector<std::pair<std::string, std::string>>{
            {"liblibidn-dynamic.so", idn},
            {"liblibiconv-dynamic.so", iconv},
                                              }));
}

TEST(TJobSharedLibrariesTest, ShipsABareLibraryLoadedFromTheWorkingDirectory)
{
    TTempFile library("libbare-" + CreateGuidAsString() + ".so");
    TFileOutput(library.Name()).Write("test");
    auto bareName = std::string(library.Name());

    auto libraries = NDetail::SelectJobSharedLibraries({bareName});

    EXPECT_EQ(ToFileNamesAndPaths(libraries), (std::vector<std::pair<std::string, std::string>>{
            {bareName, bareName},
                                              }));
}

TEST(TJobSharedLibrariesTest, RejectsAMissingBareLibrary)
{
    EXPECT_THROW_WITH_SUBSTRING(
        NDetail::SelectJobSharedLibraries({std::string("libmissing-" + CreateGuidAsString() + ".so")}),
        "is not a regular file");
}

TEST(TJobSharedLibrariesTest, KeepsTheLoadedNameOfASymlinkedLibrary)
{
    TTempDir dir;
    auto target = WriteTestFile(dir, "libfoo.so.1.2.3");
    auto link = std::string(dir.Name()) + "/libfoo.so.1";
    ASSERT_TRUE(NFs::SymLink(TString(target), TString(link)));

    auto libraries = NDetail::SelectJobSharedLibraries({link});

    EXPECT_EQ(ToFileNamesAndPaths(libraries), (std::vector<std::pair<std::string, std::string>>{
            {"libfoo.so.1", link},
                                              }));
}

TEST(TJobSharedLibrariesTest, ShipsALibraryLoadedTwiceOnce)
{
    TTempDir dir;
    auto idn = WriteTestFile(dir, "liblibidn-dynamic.so");

    auto libraries = NDetail::SelectJobSharedLibraries({idn, idn});

    EXPECT_EQ(libraries.size(), 1u);
}

TEST(TJobSharedLibrariesTest, RejectsLibrariesSharingAFileName)
{
    TTempDir first;
    TTempDir second;

    EXPECT_THROW_WITH_SUBSTRING(
        NDetail::SelectJobSharedLibraries({
            WriteTestFile(first, "libsame.so"),
            WriteTestFile(second, "libsame.so"),
        }),
        "share the file name");
}

TEST(TJobSharedLibrariesTest, RejectsAMissingLibrary)
{
    TTempDir dir;

    EXPECT_THROW_WITH_SUBSTRING(
        NDetail::SelectJobSharedLibraries({std::string(dir.Name()) + "/libmissing.so"}),
        "is not a regular file");
}

////////////////////////////////////////////////////////////////////////////////

struct TResolvedJobFile
{
    std::string FileName;
    std::string LocalPath;
};

class TBuildVanillaTaskSpecTest
    : public ::testing::Test
{
protected:
    TTempDir Dir_;
    std::string NodeConfigPath_ = WriteTestFile(Dir_, "node_config");
    std::string UserFilePath_ = WriteTestFile(Dir_, "user_file");
    std::vector<NDetail::TJobSharedLibrary> Libraries_ = {
        {"liblibidn-dynamic.so", WriteTestFile(Dir_, "liblibidn-dynamic.so")},
        {"liblibiconv-dynamic.so", WriteTestFile(Dir_, "liblibiconv-dynamic.so")},
    };
    std::vector<TResolvedJobFile> Resolved_;

    TFlowVanillaTask MakeTask() const
    {
        TFlowVanillaTask task;
        task.JobCount = 1;
        task.Environment["CUSTOM"] = "value";
        task.LocalFiles["user_file"] = UserFilePath_;
        task.CypressFiles["cypress_file"] = "//tmp/cypress_file";
        return task;
    }

    TVanillaTaskSpec Build(
        const TFlowVanillaTask& task,
        const std::optional<std::string>& localBinaryPath,
        const std::vector<NDetail::TJobSharedLibrary>& libraries,
        const std::string& cypressDir = "//cache")
    {
        return NDetail::BuildVanillaTaskSpec(
            "worker",
            "Worker",
            task,
            /*networkProject*/ std::nullopt,
            NodeConfigPath_,
            localBinaryPath,
            libraries,
            [&] (const std::string& fileName, const std::string& localPath, bool /*executable*/) {
                Resolved_.push_back({fileName, localPath});
                return NYPath::TYPath(cypressDir + "/" + fileName);
            });
    }

    static THashMap<std::string, std::string> GetFilePaths(const TVanillaTaskSpec& taskSpec)
    {
        THashMap<std::string, std::string> result;
        for (const auto& file : taskSpec.Files) {
            EXPECT_TRUE(result.emplace(file.FileName, file.CypressPath).second) << file.FileName;
        }
        return result;
    }
};

TEST_F(TBuildVanillaTaskSpecTest, ShipsSharedLibrariesWithTheUploadedBinary)
{
    auto taskSpec = Build(MakeTask(), /*localBinaryPath*/ std::nullopt, Libraries_);

    EXPECT_EQ(GetFilePaths(taskSpec), (THashMap<std::string, std::string>{
            {"flow_server", "//cache/flow_server"},
            {"node_config", "//cache/node_config"},
            {"user_file", "//cache/user_file"},
            {"cypress_file", "//tmp/cypress_file"},
            {"flow_libraries/liblibidn-dynamic.so", "//cache/flow_libraries/liblibidn-dynamic.so"},
            {"flow_libraries/liblibiconv-dynamic.so", "//cache/flow_libraries/liblibiconv-dynamic.so"},
                                      }));
    THashMap<std::string, std::string> resolvedLibraryPaths;
    for (const auto& file : Resolved_) {
        resolvedLibraryPaths[file.FileName] = file.LocalPath;
    }
    EXPECT_EQ(resolvedLibraryPaths.at("flow_libraries/liblibidn-dynamic.so"), Libraries_[0].LocalPath);
    EXPECT_EQ(resolvedLibraryPaths.at("flow_libraries/liblibiconv-dynamic.so"), Libraries_[1].LocalPath);
    EXPECT_EQ(taskSpec.Environment, (THashMap<std::string, std::string>{
            {"CUSTOM", "value"},
            {"LD_LIBRARY_PATH", "./flow_libraries"},
                                    }));
}

TEST_F(TBuildVanillaTaskSpecTest, PrependsLibrariesToTheConfiguredSearchPath)
{
    auto task = MakeTask();
    task.Environment["LD_LIBRARY_PATH"] = "/opt/custom";

    auto taskSpec = Build(task, /*localBinaryPath*/ std::nullopt, Libraries_);

    EXPECT_EQ(taskSpec.Environment.at("LD_LIBRARY_PATH"), "./flow_libraries:/opt/custom");
}

TEST_F(TBuildVanillaTaskSpecTest, LeavesTheSearchPathAloneWithoutLibraries)
{
    auto taskSpec = Build(MakeTask(), /*localBinaryPath*/ std::nullopt, /*libraries*/ {});

    EXPECT_FALSE(taskSpec.Environment.contains("LD_LIBRARY_PATH"));
    EXPECT_EQ(taskSpec.Files.size(), 4u);
}

TEST_F(TBuildVanillaTaskSpecTest, RunsALocalBinaryWithItsOwnLibraries)
{
    auto taskSpec = Build(MakeTask(), std::string("/build/flow_server"), Libraries_);

    EXPECT_EQ(GetFilePaths(taskSpec), (THashMap<std::string, std::string>{
            {"node_config", "//cache/node_config"},
            {"user_file", "//cache/user_file"},
            {"cypress_file", "//tmp/cypress_file"},
                                      }));
    EXPECT_EQ(taskSpec.Environment, (THashMap<std::string, std::string>{{"CUSTOM", "value"}}));
    EXPECT_EQ(taskSpec.Command, "/build/flow_server --config node_config");
}

TEST_F(TBuildVanillaTaskSpecTest, RejectsUserFilesInTheLibrariesDirectory)
{
    auto task = MakeTask();
    task.LocalFiles["flow_libraries/liblibidn-dynamic.so"] = UserFilePath_;
    EXPECT_THROW_WITH_SUBSTRING(
        Build(task, /*localBinaryPath*/ std::nullopt, Libraries_),
        "collides with the shared libraries directory");

    task = MakeTask();
    task.CypressFiles["flow_libraries"] = "//tmp/flow_libraries";
    EXPECT_THROW_WITH_SUBSTRING(
        Build(task, /*localBinaryPath*/ std::nullopt, Libraries_),
        "collides with the shared libraries directory");
}

TEST_F(TBuildVanillaTaskSpecTest, PersistedSpecKeepsLibraryNamesAndSearchPath)
{
    TVanillaSpec spec;
    spec.Tasks.push_back(Build(MakeTask(), /*localBinaryPath*/ std::nullopt, Libraries_, "//pipeline/vanilla/files"));
    auto worker = BuildVanillaOperationSpec(spec)->GetChildOrThrow("tasks")->AsMap()->GetChildOrThrow("worker")->AsMap();

    THashMap<std::string, std::string> filePaths;
    for (const auto& file : worker->GetChildOrThrow("file_paths")->AsList()->GetChildren()) {
        filePaths[file->Attributes().Get<std::string>("file_name")] = file->AsString()->GetValue();
    }
    EXPECT_EQ(filePaths.at("flow_libraries/liblibidn-dynamic.so"), "//pipeline/vanilla/files/flow_libraries/liblibidn-dynamic.so");
    EXPECT_EQ(filePaths.at("flow_libraries/liblibiconv-dynamic.so"), "//pipeline/vanilla/files/flow_libraries/liblibiconv-dynamic.so");
    auto environment = ConvertTo<THashMap<std::string, std::string>>(worker->GetChildOrThrow("environment"));
    EXPECT_EQ(environment.at("LD_LIBRARY_PATH"), "./flow_libraries");
    EXPECT_EQ(environment.at("CUSTOM"), "value");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TVanillaLauncherClientsTest, TakesPipelineClientFromClientsCache)
{
    auto cache = New<TAssertClientsCache>(/*expectedCluster*/ TStringBuf(), MakeCacheWithPinnedProxy());

    // Serving nothing is enough to stop the launch, so the cache is the launcher's only way of
    // reaching the pipeline cluster — asked for under the runner's own role.
    EXPECT_THROW_WITH_SUBSTRING(
        LaunchInVanillaJob(MakePipelinePath(), /*proxyRole*/ "flow", MakeVanillaConfig(), cache),
        Format("%v \"pipeline-cluster/flow\"", StopMarker));
}

TEST(TVanillaLauncherClientsTest, TakesRuntimeClientFromClientsCacheUnderItsOwnRole)
{
    auto underlying = MakeCacheWithPinnedProxy();
    auto cache = New<TAssertClientsCache>(PipelineCluster, underlying);

    // Serving the pipeline cluster lets the launch reach the runtime one, which it asks for under
    // the role configured for it rather than the runner's own.
    EXPECT_THROW_WITH_SUBSTRING(
        LaunchInVanillaJob(
            MakePipelinePath(),
            /*proxyRole*/ {},
            MakeVanillaConfig("runtime_cluster=runtime-cluster; runtime_proxy_role=heavy"),
            cache),
        Format("%v \"runtime-cluster/heavy\"", StopMarker));

    // Those are two distinct keys in a cache that holds one client per cluster URL, so the runtime
    // cluster is reached with a client of its own rather than the pipeline's.
    EXPECT_NE(underlying->GetClient(PipelineCluster), underlying->GetClient("runtime-cluster/heavy"));
}

//! What the whole change is for: the launcher reaches the cluster over the connection the runner
//! configured. Building its own client from the cluster URL, as it used to, left proxy discovery
//! as the only way in — and a cluster whose advertised proxy address does not resolve on the
//! runner's host is then unreachable, however the runner is configured.
TEST(TVanillaLauncherClientsTest, PipelineClientUsesTheConfiguredConnection)
{
    auto underlying = MakeCacheWithPinnedProxy();
    auto cache = New<TAssertClientsCache>(PipelineCluster, underlying);

    // Stopped at the runtime cluster, i.e. only after the pipeline client had been handed over.
    EXPECT_THROW_WITH_SUBSTRING(
        LaunchInVanillaJob(
            MakePipelinePath(),
            /*proxyRole*/ {},
            MakeVanillaConfig("runtime_cluster=runtime-cluster"),
            cache),
        Format("%v \"runtime-cluster\"", StopMarker));

    // The cache holds one client per cluster URL, so this is the very client the launcher got.
    auto connectionConfig = ConvertTo<IMapNodePtr>(
        underlying->GetClient(PipelineCluster)->GetConnection()->GetConfigYson());
    EXPECT_FALSE(ConvertTo<bool>(connectionConfig->GetChildOrThrow("enable_proxy_discovery")));
    EXPECT_EQ(
        std::vector<std::string>{std::string(PinnedProxyAddress)},
        ConvertTo<std::vector<std::string>>(connectionConfig->GetChildOrThrow("proxy_addresses")));
}

TEST(TVanillaLauncherAclTest, DisabledLaunchAcquiresNoClient)
{
    auto cache = New<TAssertClientsCache>(/*expectedCluster*/ TStringBuf(), MakeCacheWithPinnedProxy());
    auto config = MakeVanillaConfig("acl=[{action=allow;subjects=[readers];permissions=[read]}]");
    config->Enable = false;

    auto handle = LaunchInVanillaJob(MakePipelinePath(), /*proxyRole*/ {}, config, cache);

    EXPECT_FALSE(handle.Client);
}

TEST(TVanillaLauncherAclTest, RejectsInvalidExplicitAclBeforeAnyRequest)
{
    auto cache = New<TAssertClientsCache>(PipelineCluster, MakeCacheWithPinnedProxy());
    auto config = MakeVanillaConfig();
    config->Acl = ConvertTo<NSecurityClient::TSerializableAccessControlList>(
        TYsonStringBuf(R"([{action=allow;subjects=[writers];permissions=[read;write]}])"));

    EXPECT_THROW_WITH_SUBSTRING(
        LaunchInVanillaJob(MakePipelinePath(), /*proxyRole*/ {}, config, cache),
        "permissions are allowed in operation ACL");
}

class TRecordingAclClient
    : public NApi::TNotImplementedClient
{
public:
    std::vector<TErrorOr<NApi::TCheckPermissionByAclResult>> PermissionCheckResults;

    std::vector<std::string> Calls;
    std::vector<std::optional<std::string>> CheckedUsers;
    std::vector<bool> IgnoredMissingSubjects;
    std::vector<INodePtr> CheckedAcls;

    TFuture<NApi::TCheckPermissionByAclResult> CheckPermissionByAcl(
        const std::optional<std::string>& user,
        EPermission permission,
        INodePtr acl,
        const NApi::TCheckPermissionByAclOptions& options) override
    {
        auto checkIndex = std::ssize(CheckedUsers);
        Calls.push_back(Format("CheckPermissionByAcl %lv", permission));
        CheckedUsers.push_back(user);
        IgnoredMissingSubjects.push_back(options.IgnoreMissingSubjects);
        CheckedAcls.push_back(std::move(acl));
        if (checkIndex >= std::ssize(PermissionCheckResults)) {
            return MakeFuture<NApi::TCheckPermissionByAclResult>(TError("Unexpected permission check %v", checkIndex));
        }
        return MakeFuture(PermissionCheckResults[checkIndex]);
    }
};

DEFINE_REFCOUNTED_TYPE(TRecordingAclClient)

class TFixedClientsCache
    : public IClientsCache
{
public:
    std::vector<std::string> RequestedClusterUrls;

    explicit TFixedClientsCache(NApi::IClientPtr client)
        : Client_(std::move(client))
    { }

    NApi::IClientPtr GetClient(TStringBuf clusterUrl) override
    {
        RequestedClusterUrls.emplace_back(clusterUrl);
        return Client_;
    }

private:
    const NApi::IClientPtr Client_;
};

DEFINE_REFCOUNTED_TYPE(TFixedClientsCache)

TError LaunchWithRecordingClient(const TIntrusivePtr<TRecordingAclClient>& client, TStringBuf configExtra = {})
{
    auto cache = New<TFixedClientsCache>(client);
    try {
        LaunchInVanillaJob(MakePipelinePath(), /*proxyRole*/ {}, MakeVanillaConfig(configExtra), cache);
    } catch (const TErrorException& ex) {
        EXPECT_EQ(cache->RequestedClusterUrls, (std::vector<std::string>(2, std::string(PipelineCluster))));
        return ex.Error();
    }
    ADD_FAILURE() << "Launch was expected to fail";
    return {};
}

void ExpectLaunchSkipsAclChecks(TStringBuf configExtra)
{
    auto client = New<TRecordingAclClient>();

    auto error = LaunchWithRecordingClient(client, configExtra);

    EXPECT_EQ(client->Calls, std::vector<std::string>{});
    EXPECT_THAT(ToString(error), testing::HasSubstr("Not implemented method CreateNode"));
}

TEST(TVanillaLauncherAclTest, SkipsAclChecksForOmittedAcl)
{
    ExpectLaunchSkipsAclChecks(/*configExtra*/ {});
}

TEST(TVanillaLauncherAclTest, SkipsAclChecksForEntityAcl)
{
    ExpectLaunchSkipsAclChecks("acl=#");
}

TEST(TVanillaLauncherAclTest, SkipsRuntimeAclCheckForEmptyAcl)
{
    ExpectLaunchSkipsAclChecks("acl=[]");
}

TEST(TVanillaLauncherAclTest, ReportsRuntimeAclCheckFailure)
{
    auto client = New<TRecordingAclClient>();
    client->PermissionCheckResults = {
        NApi::TCheckPermissionByAclResult{.Action = NSecurityClient::ESecurityAction::Allow},
        TError("Permission check RPC failed"),
    };

    auto error = LaunchWithRecordingClient(client, "acl=[{action=allow;subjects=[readers];permissions=[read]}]");

    std::vector<std::string> expectedCalls{"CheckPermissionByAcl read", "CheckPermissionByAcl manage"};
    EXPECT_EQ(client->Calls, expectedCalls);
    EXPECT_EQ(error.GetMessage(), "Cannot use vanilla operation ACL on runtime cluster \"pipeline-cluster\": ACL check failed");
    ASSERT_EQ(error.InnerErrors().size(), 1u);
    EXPECT_THAT(ToString(error.InnerErrors()[0]), testing::HasSubstr("Permission check RPC failed"));
}

TEST(TVanillaLauncherAclTest, ReportsFirstTenMissingRuntimeSubjects)
{
    auto subject = [] (int index) {
        return Format("subject-%02d", index);
    };
    auto client = New<TRecordingAclClient>();
    client->PermissionCheckResults = {
        NApi::TCheckPermissionByAclResult{.MissingSubjects = {subject(12), subject(3), subject(1), subject(7)}},
        NApi::TCheckPermissionByAclResult{.MissingSubjects = {subject(3), subject(11), subject(2), subject(9), subject(5)}},
        NApi::TCheckPermissionByAclResult{.MissingSubjects = {subject(1), subject(10), subject(4), subject(6), subject(8), subject(12)}},
    };

    std::string aclSubjects;
    for (int index = 1; index <= 12; ++index) {
        aclSubjects += Format("%Qv;", subject(index));
    }
    auto aclYson = Format("[{action=allow;subjects=[%v];permissions=[read]}]", aclSubjects);

    auto error = LaunchWithRecordingClient(client, Format("acl=%v", aclYson));

    std::vector<std::string> expectedCalls{
        "CheckPermissionByAcl read",
        "CheckPermissionByAcl manage",
        "CheckPermissionByAcl administer",
    };
    EXPECT_EQ(client->Calls, expectedCalls);
    EXPECT_EQ(client->CheckedUsers, (std::vector<std::optional<std::string>>(3, std::nullopt)));
    EXPECT_EQ(client->IgnoredMissingSubjects, std::vector<bool>(3, true));
    auto expectedAcl = ConvertToNode(ConvertTo<NSecurityClient::TSerializableAccessControlList>(TYsonStringBuf(aclYson)));
    for (const auto& acl : client->CheckedAcls) {
        EXPECT_TRUE(AreNodesEqual(acl, expectedAcl));
    }

    std::vector<std::string> reportedSubjects;
    for (int index = 1; index <= 10; ++index) {
        reportedSubjects.push_back(subject(index));
    }
    EXPECT_EQ(
        error.GetMessage(),
        Format("Cannot use vanilla operation ACL on runtime cluster \"pipeline-cluster\": subjects %v do not exist there", reportedSubjects));
    EXPECT_EQ(error.Attributes().Get<int>("missing_subject_count"), 12);
}

////////////////////////////////////////////////////////////////////////////////

//! The secure vault is only assembled once the binary is uploaded, so a missing secret is reported
//! up front: the launch names the variable rather than stopping at the first cluster it asks for.
TEST(TVanillaLauncherSecretEnvTest, RejectsUnsetSecretEnvBeforeReachingCluster)
{
    auto cache = New<TAssertClientsCache>(/*expectedCluster*/ TStringBuf(), MakeCacheWithPinnedProxy());

    EXPECT_THROW_WITH_SUBSTRING(
        LaunchInVanillaJob(
            MakePipelinePath(),
            /*proxyRole*/ {},
            MakeVanillaConfig("secret_env=[FLOW_UT_UNSET_SECRET]"),
            cache),
        "FLOW_UT_UNSET_SECRET");
}

TEST(TVanillaLauncherSecretEnvTest, AcceptsSetSecretEnv)
{
    SetEnv("FLOW_UT_SECRET", "value");
    auto cache = New<TAssertClientsCache>(/*expectedCluster*/ TStringBuf(), MakeCacheWithPinnedProxy());

    // Stopped at the cluster, i.e. the declared secret let the launch through.
    EXPECT_THROW_WITH_SUBSTRING(
        LaunchInVanillaJob(
            MakePipelinePath(),
            /*proxyRole*/ {},
            MakeVanillaConfig("secret_env=[FLOW_UT_SECRET]"),
            cache),
        Format("%v \"pipeline-cluster\"", StopMarker));
}

////////////////////////////////////////////////////////////////////////////////

// The patch is merged into a fully serialized default config, and all three backends spell their
// leader lease the same way. A patch that switches the backend must therefore not leave the
// previous backend's values behind: the Cypress TTL is five seconds, while chaos wants its own
// minute and dyntable would not even load with five.

TEST(TPatchVanillaNodeConfigTest, SwitchingToChaosKeepsTheChaosDefaults)
{
    auto nodeConfig = BuildDefaultVanillaNodeConfig(
        MakePipelinePath(),
        /*proxyRole*/ {},
        /*workerPortCount*/ {});
    ASSERT_EQ(nodeConfig->Controller->ElectionManager.GetType(), NController::EElectionBackend::Cypress);

    auto patched = PatchVanillaNodeConfig(
        nodeConfig,
        NYTree::ConvertToNode(NYson::TYsonString(TStringBuf(
            "{controller={election_manager={backend=chaos;chaos_cell_bundle=\"test-bundle\"}}}"))));

    const auto& electionManager = patched->Controller->ElectionManager;
    ASSERT_EQ(electionManager.GetType(), NController::EElectionBackend::Chaos);
    auto backendConfig = electionManager.GetConcrete<NController::TChaosElectionBackendConfig>();
    EXPECT_EQ(backendConfig->ChaosCellBundle, "test-bundle");
    EXPECT_EQ(backendConfig->LeaderLeaseTtl, TDuration::Minutes(1));
    EXPECT_EQ(backendConfig->LeaderLeasePingPeriod, TDuration::Seconds(12));
}

TEST(TPatchVanillaNodeConfigTest, SwitchingToDyntableKeepsTheDyntableDefaults)
{
    auto nodeConfig = BuildDefaultVanillaNodeConfig(
        MakePipelinePath(),
        /*proxyRole*/ {},
        /*workerPortCount*/ {});

    auto patched = PatchVanillaNodeConfig(
        nodeConfig,
        NYTree::ConvertToNode(NYson::TYsonString(TStringBuf(
            "{controller={election_manager={backend=dyntable}}}"))));

    const auto& electionManager = patched->Controller->ElectionManager;
    ASSERT_EQ(electionManager.GetType(), NController::EElectionBackend::Dyntable);
    auto backendConfig = electionManager.GetConcrete<NController::TDyntableElectionBackendConfig>();
    EXPECT_EQ(backendConfig->LeaderLeaseTtl, TDuration::Minutes(1));
    EXPECT_EQ(backendConfig->DetachTimeout, TDuration::Minutes(1));
}

// A patch that does not name a backend keeps refining the one already there.
TEST(TPatchVanillaNodeConfigTest, KeepsTheElectionSettingsWhenTheBackendStays)
{
    auto nodeConfig = BuildDefaultVanillaNodeConfig(
        MakePipelinePath(),
        /*proxyRole*/ {},
        /*workerPortCount*/ {});

    auto patched = PatchVanillaNodeConfig(
        nodeConfig,
        NYTree::ConvertToNode(NYson::TYsonString(TStringBuf(
            "{controller={election_manager={lock_acquisition_period=\"7s\"}}}"))));

    const auto& electionManager = patched->Controller->ElectionManager;
    ASSERT_EQ(electionManager.GetType(), NController::EElectionBackend::Cypress);
    auto backendConfig = electionManager.GetConcrete<NController::TCypressElectionBackendConfig>();
    EXPECT_EQ(backendConfig->LockAcquisitionPeriod, TDuration::Seconds(7));
    EXPECT_EQ(backendConfig->LeaderLeaseTtl, TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
