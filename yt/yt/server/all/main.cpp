#include <yt/yt/server/master/cell_master/program.h>
#include <yt/yt/server/clock_server/cluster_clock/program.h>
#include <yt/yt/server/http_proxy/program.h>
#include <yt/yt/server/rpc_proxy/program.h>
#include <yt/yt/server/job_proxy/program.h>
#include <yt/yt/server/scheduler/program.h>
#include <yt/yt/server/controller_agent/program.h>
#include <yt/yt/server/tools/program.h>
#include <yt/yt/server/node/cluster_node/program.h>
#include <yt/yt/server/exec/program.h>
#include <yt/yt/server/log_tailer/program.h>
#include <yt/yt/server/discovery_server/program.h>
#include <yt/yt/server/timestamp_provider/program.h>
#include <yt/yt/server/master_cache/program.h>
#include <yt/yt/server/cell_balancer/program.h>
#include <yt/yt/server/queue_agent/program.h>
#include <yt/yt/server/query_tracker/program.h>
#include <yt/yt/server/tablet_balancer/program.h>
#include <yt/yt/server/cypress_proxy/program.h>
#include <yt/yt/server/tcp_proxy/program.h>
#include <yt/yt/server/kafka_proxy/program.h>
#include <yt/yt/server/replicated_table_tracker/program.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/core/misc/fs.h>

#include <library/cpp/yt/system/exit.h>

#include <library/cpp/getopt/small/last_getopt_parse_result.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

constexpr auto CommandPrefix = TStringBuf("ytserver-");
constexpr auto AllCommand = TStringBuf("ytserver-all");

////////////////////////////////////////////////////////////////////////////////

using TProgramRunner = std::function<void(int argc, const char** argv)>;

class TProgramMap
{
public:
    std::vector<std::string> GetNames() const
    {
        return GetKeys(NameToRunner_);
    }

    bool TryRun(int argc, const char** argv) const
    {
        // TODO(babenko): migrate to std::string
        auto command = std::string(NFS::GetFileName(TString(argv[0])));
        if (!command.starts_with(CommandPrefix)) {
            return false;
        }

        auto suffix = command.substr(CommandPrefix.size());
        auto it = NameToRunner_.find(suffix);
        if (it == NameToRunner_.end()) {
            return false;
        }

        it->second(argc, argv);
        return true;
    }

private:
    friend class TProgramMapBuilder;

    explicit TProgramMap(THashMap<std::string, TProgramRunner> nameToRunner)
        : NameToRunner_(std::move(nameToRunner))
    { }

    const THashMap<std::string, TProgramRunner> NameToRunner_;
};

////////////////////////////////////////////////////////////////////////////////

class TProgramMapBuilder
{
public:
    template <class T>
    TProgramMapBuilder Add(const std::string& name) &&
    {
        EmplaceOrCrash(
            NameToRunner_,
            name,
            [] (int argc, const char** argv) {
                T().Run(argc, argv);
            });
        return std::move(*this);
    }

    TProgramMap Finish() &&
    {
        return TProgramMap(std::move(NameToRunner_));
    }

private:
    THashMap<std::string, TProgramRunner> NameToRunner_;
};

////////////////////////////////////////////////////////////////////////////////

const TProgramMap& GetProgramMap()
{
    static const auto result = [] {
        return TProgramMapBuilder()
            .Add<NCellMaster::TCellMasterProgram>("master")
            .Add<NClusterClock::TClusterClockProgram>("clock")
            .Add<NHttpProxy::THttpProxyProgram>("http-proxy")
            // TODO(babenko): rename to rpc-proxy
            .Add<NRpcProxy::TRpcProxyProgram>("proxy")
            .Add<NClusterNode::TClusterNodeProgram>("node")
            .Add<NJobProxy::TJobProxyProgram>("job-proxy")
            .Add<NExec::TExecProgram>("exec")
            .Add<NTools::TToolsProgram>("tools")
            .Add<NScheduler::TSchedulerProgram>("scheduler")
            .Add<NControllerAgent::TControllerAgentProgram>("controller-agent")
            .Add<NLogTailer::TLogTailerProgram>("log-tailer")
            .Add<NClusterDiscoveryServer::TClusterDiscoveryServerProgram>("discovery")
            .Add<NTimestampProvider::TTimestampProviderProgram>("timestamp-provider")
            .Add<NMasterCache::TMasterCacheProgram>("master-cache")
            .Add<NCellBalancer::TCellBalancerProgram>("cell-balancer")
            .Add<NQueueAgent::TQueueAgentProgram>("queue-agent")
            .Add<NTabletBalancer::TTabletBalancerProgram>("tablet-balancer")
            .Add<NCypressProxy::TCypressProxyProgram>("cypress-proxy")
            .Add<NQueryTracker::TQueryTrackerProgram>("query-tracker")
            .Add<NTcpProxy::TTcpProxyProgram>("tcp-proxy")
            .Add<NKafkaProxy::TKafkaProxyProgram>("kafka-proxy")
            .Add<NReplicatedTableTracker::TReplicatedTableTrackerProgram>("replicated-table-tracker")
            .Finish();
    }();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TAllProgram
    : public TProgram
{
public:
    TAllProgram()
    {
        TStringBuilder allNamesBuilder;
        for (const auto& name : GetProgramMap().GetNames()) {
            allNamesBuilder.AppendFormat("ytserver-%v\n", name);
        }

        // Fake option just to show in --help output.
        Opts_
            .AddFreeArgBinding("program-name", ProgramName_, "Program name to run");
        Opts_
            .SetFreeArgsMax(Opts_.UNLIMITED_ARGS);
        Opts_
            .AddSection("Programs", allNamesBuilder.Flush());
    }

private:
    TString ProgramName_;

    void DoRun(const NLastGetopt::TOptsParseResult& /*result*/) override
    {
        Cerr << "Program " << Argv0_ << " is not known" << Endl;
        Exit(ToUnderlying(EProcessExitCode::ArgumentsError));
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char** argv)
{
    using namespace NYT;

    // Shift arguments to handle "program-name" specified in the first argument.
    // Example: ./ytserver-all ytserver-master --help
    if (argc >= 2 &&
        TStringBuf(argv[0]).EndsWith(AllCommand) &&
        TStringBuf(argv[1]).StartsWith(CommandPrefix))
    {
        argc--;
        argv++;
    }

    if (!GetProgramMap().TryRun(argc, argv)) {
        // Handles auxiliary flags like --version and --build.
        TAllProgram().Run(argc, argv);
    }
}

////////////////////////////////////////////////////////////////////////////////
