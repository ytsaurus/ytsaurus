#include <yt/yt/flow/library/cpp/vanilla/current_operation.h>

#include <yt/yt/client/cache/rpc.h>

#include <yt/yt/library/program/program.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Brings a vanilla-launched pipeline back up from the manifest the launcher persisted, reusing
//! exactly the same operation spec (and thus the same binary version): it reads the persisted spec,
//! refuses if the recorded operation is still alive (so it never starts a second one), and resubmits
//! it — optionally on a different runtime cluster.
class TReanimateVanillaOperationProgram
    : public virtual TProgram
{
public:
    TReanimateVanillaOperationProgram()
    {
        Opts_.AddLongOption("cluster", "Pipeline YT cluster")
            .StoreResult(&Cluster_)
            .Required();
        Opts_.AddLongOption("path", "Cypress path of the pipeline")
            .StoreResult(&Path_)
            .Required();
        Opts_.AddLongOption("proxy-role", "RPC proxy role")
            .StoreResult(&ProxyRole_)
            .Optional();
        Opts_.AddLongOption("runtime-cluster", "Cluster to (re)start the operation on; defaults to the recorded one")
            .StoreResult(&RuntimeCluster_)
            .Optional();
        Opts_.AddLongOption("runtime-proxy-role", "RPC proxy role for the runtime cluster; defaults to the recorded one")
            .StoreResult(&RuntimeProxyRole_)
            .Optional();
    }

protected:
    void DoRun() override
    {
        auto pipelineClient = NClient::NCache::CreateClient(Cluster_, ProxyRole_);
        ReanimateVanillaOperation(pipelineClient, Cluster_, Path_, RuntimeCluster_, RuntimeProxyRole_);
    }

private:
    std::string Cluster_;
    NYPath::TYPath Path_;
    std::optional<std::string> ProxyRole_;
    std::optional<std::string> RuntimeCluster_;
    std::optional<std::string> RuntimeProxyRole_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

int main(int argc, const char** argv)
{
    return NYT::NFlow::TReanimateVanillaOperationProgram().Run(argc, argv);
}
