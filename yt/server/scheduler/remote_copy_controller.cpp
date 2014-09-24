#include "stdafx.h"
#include "remote_copy_controller.h"
#include "private.h"
#include "helpers.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "job_resources.h"

#include <core/ytree/fluent.h>

#include <core/rpc/helpers.h>

#include <ytlib/api/connection.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/chunk_slice.h>

#include <ytlib/node_tracker_client/node_directory.h>
#include <ytlib/node_tracker_client/node_directory_builder.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <server/job_proxy/config.h>

namespace NYT {
namespace NScheduler {

using namespace NYson;
using namespace NYTree;
using namespace NYPath;
using namespace NChunkServer;
using namespace NJobProxy;
using namespace NChunkClient;
using namespace NScheduler::NProto;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NApi;

////////////////////////////////////////////////////////////////////

static NProfiling::TProfiler Profiler("/operations/remote_copy");

////////////////////////////////////////////////////////////////////

class TRemoteCopyController
    : public TOperationControllerBase
{
public:
    TRemoteCopyController(
        TSchedulerConfigPtr config,
        TRemoteCopyOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TOperationControllerBase(config, spec, host, operation)
        , Spec_(spec)
    { }

    virtual void BuildBriefSpec(IYsonConsumer* consumer) const override
    {
        TOperationControllerBase::BuildBriefSpec(consumer);
        BuildYsonMapFluently(consumer)
            .Item("cluster_name").Value(Spec_->ClusterName)
            .Item("network_name").Value(Spec_->NetworkName);
    }

    // Persistence.

    virtual void Persist(TPersistenceContext& context) override
    {
        TOperationControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, RemoteCopyTask_);
        Persist(context, RemoteCopyTaskGroup_);
        Persist(context, JobIOConfig_);
        Persist(context, JobSpecTemplate_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TRemoteCopyController, 0xbac5ad82);

    TRemoteCopyOperationSpecPtr Spec_;

    class TRemoteCopyTask
        : public TTask
    {
    public:
        //! For persistence only.
        TRemoteCopyTask()
            : Controller_(nullptr)
        { }

        TRemoteCopyTask(
                TRemoteCopyController* controller,
                int jobCount)
            : TTask(controller)
            , Controller_(controller)
            , ChunkPool_(CreateUnorderedChunkPool(Controller_->NodeDirectory, jobCount))
        { }

        virtual Stroka GetId() const override
        {
            return "RemoteCopy";
        }

        virtual TTaskGroupPtr GetGroup() const override
        {
            return Controller_->RemoteCopyTaskGroup_;
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return TDuration::Zero();
        }

        virtual TNodeResources GetNeededResources(TJobletPtr joblet) const override
        {
            return GetRemoteCopyResources(
                joblet->InputStripeList->GetStatistics(),
                joblet->MemoryReserveEnabled);
        }

        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return ChunkPool_.get();
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return ChunkPool_.get();
        }

        virtual void Persist(TPersistenceContext& context) override
        {
            TTask::Persist(context);

            using NYT::Persist;
            Persist(context, Controller_);
            Persist(context, ChunkPool_);
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TRemoteCopyTask, 0x83b0dfe3);

        TRemoteCopyController* Controller_;

        std::unique_ptr<IChunkPool> ChunkPool_;

        virtual bool IsMemoryReserveEnabled() const override
        {
            return Controller_->IsMemoryReserveEnabled(Controller_->JobCounter);
        }

        virtual TNodeResources GetMinNeededResourcesHeavy() const override
        {
            return GetRemoteCopyResources(
                ChunkPool_->GetApproximateStripeStatistics(),
                IsMemoryReserveEnabled());
        }

        TNodeResources GetRemoteCopyResources(const TChunkStripeStatisticsVector& statistics, bool isReserveEnabled) const
        {
            TNodeResources result;
            result.set_user_slots(1);
            result.set_cpu(0);
            result.set_memory(GetMemoryResources(statistics));
            return result;
        }

        i64 GetMemoryResources(const TChunkStripeStatisticsVector& statistics) const
        {
            i64 result = 0;

            // Replication writer
            result += Controller_->Spec_->JobIO->TableWriter->SendWindowSize +
                Controller_->Spec_->JobIO->TableWriter->GroupSize;

            // Max block size
            i64 maxBlockSize = 0;
            for (const auto& stat : statistics) {
                 maxBlockSize = std::max(maxBlockSize, stat.MaxBlockSize);
            }
            result += maxBlockSize;

            // Memory footprint
            result += GetFootprintMemorySize();

            return result;
        }

        virtual int GetChunkListCountPerJob() const override
        {
            return 1;
        }

        virtual EJobType GetJobType() const override
        {
            return EJobType(Controller_->JobSpecTemplate_.type());
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller_->JobSpecTemplate_);

            auto* remoteCopyJobSpecExt = jobSpec->MutableExtension(TRemoteCopyJobSpecExt::remote_copy_job_spec_ext);
            auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            NNodeTrackerClient::TNodeDirectoryBuilder directoryBuilder(
                Controller_->NodeDirectory,
                remoteCopyJobSpecExt->mutable_remote_node_directory());

            auto* inputSpec = schedulerJobSpecExt->add_input_specs();
            auto list = joblet->InputStripeList;
            for (const auto& stripe : list->Stripes) {
                for (const auto& chunkSlice : stripe->ChunkSlices) {
                    auto* chunkSpec = inputSpec->add_chunks();
                    ToProto(chunkSpec, *chunkSlice);
                    for (ui32 protoReplica : chunkSlice->GetChunkSpec()->replicas()) {
                        auto replica = FromProto<NChunkClient::TChunkReplica>(protoReplica);
                        directoryBuilder.Add(replica);
                    }
                }
            }
            UpdateInputSpecTotals(jobSpec, joblet);

            AddFinalOutputSpecs(jobSpec, joblet);
        }

        virtual void OnJobCompleted(TJobletPtr joblet) override
        {
            TTask::OnJobCompleted(joblet);
            RegisterOutput(joblet, joblet->JobIndex);
        }

        virtual void OnJobAborted(TJobletPtr joblet) override
        {
            TTask::OnJobAborted(joblet);
            Controller_->UpdateAllTasksIfNeeded(Controller_->JobCounter);
        }

    };

    typedef TIntrusivePtr<TRemoteCopyTask> TRemoteCopyTaskPtr;

    TRemoteCopyTaskPtr RemoteCopyTask_;
    TTaskGroupPtr RemoteCopyTaskGroup_;

    TJobIOConfigPtr JobIOConfig_;
    TJobSpec JobSpecTemplate_;

    // Custom bits of preparation pipeline.
    void InitializeTransactions() override
    {
        StartAsyncSchedulerTransaction();
        if (Operation->GetCleanStart()) {
            StartInputTransaction(TTransactionId());
            auto userTransactionId =
                Operation->GetUserTransaction()
                ? Operation->GetUserTransaction()->GetId()
                : TTransactionId();
            StartOutputTransaction(userTransactionId);
        }
    }

    virtual void DoInitialize() override
    {
        TOperationControllerBase::DoInitialize();

        RemoteCopyTaskGroup_ = New<TTaskGroup>();
        RegisterTaskGroup(RemoteCopyTaskGroup_);
    }

    virtual void Essentiate() override
    {
        TClientOptions options;
        options.User = Operation->GetAuthenticatedUser();
        AuthenticatedInputMasterClient = Host
            ->GetClusterDirectory()
            ->GetConnectionOrThrow(Spec_->ClusterName)
            ->CreateClient(options);

        TOperationControllerBase::Essentiate();
    }

    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec_->InputTablePaths;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return std::vector<TRichYPath>(1, Spec_->OutputTablePath);
    }

    virtual std::vector<TPathWithStage> GetFilePaths() const override
    {
        return std::vector<TPathWithStage>();
    }

    virtual void CustomPrepare() override
    {
        TOperationControllerBase::CustomPrepare();

        if (InputTables.size() == 1) {
            OutputTables[0].Options->KeyColumns = InputTables[0].KeyColumns;
        }

        LOG_INFO("Processing inputs");

        std::vector<TChunkStripePtr> stripes;
        for (const auto& chunkSpec : CollectInputChunks()) {
            if (chunkSpec->has_lower_limit() && !IsTrivial(chunkSpec->lower_limit()) ||
                chunkSpec->has_upper_limit() && !IsTrivial(chunkSpec->upper_limit()))
            {
                OnOperationFailed(TError("Remote copy operation does not support non-trivial table limits"));
                return;
            }
            stripes.push_back(New<TChunkStripe>(CreateChunkSlice(chunkSpec)));
        }

        auto jobCount = SuggestJobCount(
            TotalInputDataSize,
            Spec_->DataSizePerJob,
            Spec_->JobCount);
        jobCount = std::min(jobCount, static_cast<int>(stripes.size()));

        if (stripes.size() > Spec_->MaxChunkCountPerJob * jobCount) {
            OnOperationFailed(TError(
                "Too many chunks per job: actual %v, limit %v; please merge input tables before starting Remote Copy",
                stripes.size() / jobCount,
                Spec_->MaxChunkCountPerJob));
            return;
        }

        RemoteCopyTask_ = New<TRemoteCopyTask>(this, jobCount);
        RemoteCopyTask_->Initialize();
        RemoteCopyTask_->AddInput(stripes);
        RemoteCopyTask_->FinishInput();
        RegisterTask(RemoteCopyTask_);

        LOG_INFO("Inputs processed");

        InitJobIOConfig();
        InitJobSpecTemplate();
    }

    virtual void CustomizeJoblet(TJobletPtr joblet) override
    { }

    virtual bool IsOutputLivePreviewSupported() const override
    {
        return false;
    }

    virtual bool IsParityReplicasFetchEnabled() const override
    {
        return true;
    }

    virtual bool IsCompleted() const override
    {
        return RemoteCopyTask_->IsCompleted();
    }

    // Progress reporting.

    virtual Stroka GetLoggingProgress() const override
    {
        return Format(
            "Jobs = {T: %v, R: %v, C: %v, P: %v, F: %v, A: %v}, "
            "UnavailableInputChunks: %v",
            JobCounter.GetTotal(),
            JobCounter.GetRunning(),
            JobCounter.GetCompleted(),
            GetPendingJobCount(),
            JobCounter.GetFailed(),
            JobCounter.GetAborted(),
            UnavailableInputChunkCount);
    }


    // Unsorted helpers.

    virtual bool NeedsAllChunkParts() const override
    {
        return true;
    }

    void InitJobIOConfig()
    {
        JobIOConfig_ = CloneYsonSerializable(Spec_->JobIO);
        InitFinalOutputConfig(JobIOConfig_);
    }

    void InitJobSpecTemplate()
    {
        JobSpecTemplate_.set_type(EJobType::RemoteCopy);
        auto* schedulerJobSpecExt = JobSpecTemplate_.MutableExtension(
            TSchedulerJobSpecExt::scheduler_job_spec_ext);

        schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(),
            Operation->GetOutputTransaction()->GetId());
        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig_).Data());

        auto clusterDirectory = Host->GetClusterDirectory();
        auto* remoteCopyJobSpecExt = JobSpecTemplate_.MutableExtension(
            TRemoteCopyJobSpecExt::remote_copy_job_spec_ext);
        remoteCopyJobSpecExt->set_connection_config(
            ConvertToYsonString(clusterDirectory->GetConnectionConfigOrThrow(Spec_->ClusterName)).Data());

        auto networkName = NNodeTrackerClient::DefaultNetworkName;
        if (Spec_->NetworkName) {
            networkName = *Spec_->NetworkName;
        } else {
            auto defaultNetwork = clusterDirectory->GetDefaultNetwork(Spec_->ClusterName);
            if (defaultNetwork) {
                networkName = *defaultNetwork;
            }
        }

        remoteCopyJobSpecExt->set_network_name(networkName);
    }

};

DEFINE_DYNAMIC_PHOENIX_TYPE(TRemoteCopyController);
DEFINE_DYNAMIC_PHOENIX_TYPE(TRemoteCopyController::TRemoteCopyTask);

IOperationControllerPtr CreateRemoteCopyController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TRemoteCopyOperationSpec>(operation->GetSpec());
    return New<TRemoteCopyController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT


