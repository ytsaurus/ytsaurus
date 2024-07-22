#include "dependency_runner.h"
#include "yt.h"
#include "yt_graph_v2.h"

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/interface/executor.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/format.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <library/cpp/yson/writer.h>
#include <util/generic/guid.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

using namespace NPrivate;
using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

class TYtExecutor
    : public IExecutor
{
public:
    explicit TYtExecutor(TYtPipelineConfig config)
        : Config_(std::move(config))
        , Client_(NYT::CreateClient(Config_.GetCluster()))
    {
        if (!Config_.GetTransactionId().empty()) {
            const TGUID guid = GetGuid(Config_.GetTransactionId());
            Tx_ = Client_->AttachTransaction(guid);
        }
    }

    void Run(const TPipeline& pipeline) override
    {
        try {
            IClientBasePtr tx = Tx_ ? static_cast<IClientBasePtr>(Tx_) : static_cast<IClientBasePtr>(Client_);

            YT_LOG_DEBUG("Transforming Roren pipeline to YT graph");

            std::shared_ptr<IYtGraph> ytGraph = BuildYtGraphV2(pipeline, Config_);

            YT_LOG_DEBUG("Optimizing YT graph");

            ytGraph->Optimize();

            YT_LOG_DEBUG("Starting execution of YT graph");

            TStartOperationContext context;
            context.Config = std::make_shared<TYtPipelineConfig>(Config_);

            auto concurrencyLimit = Config_.GetConcurrencyLimit();
            auto runner = MakeDependencyRunner(tx, std::dynamic_pointer_cast<TYtGraphV2>(ytGraph), concurrencyLimit);
            runner->RunOperations(context);

            YT_LOG_DEBUG("All operations was completed");
        } catch (...) {
            // just to make sure that tx is aborted
            throw;
        }
    }

private:
    const TYtPipelineConfig Config_;
    const NYT::IClientPtr Client_;
    ITransactionPtr Tx_;
};

////////////////////////////////////////////////////////////////////////////////

TPipeline MakeYtPipeline(const TString& cluster, const TString& workingDir)
{
    TYtPipelineConfig config;
    config.SetCluster(cluster);
    config.SetWorkingDir(workingDir);
    return MakeYtPipeline(std::move(config));
}

TPipeline MakeYtPipeline(TYtPipelineConfig config)
{
    return NPrivate::MakePipeline(MakeIntrusive<TYtExecutor>(std::move(config)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
