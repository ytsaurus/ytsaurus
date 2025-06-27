#include "remote_copy_controller.h"

#include "operation_controller_detail.h"

#include <yt/yt/server/controller_agent/config.h>
#include <yt/yt/server/controller_agent/job_size_constraints.h>
#include <yt/yt/server/controller_agent/operation.h>

#include <yt/yt/server/lib/chunk_pools/ordered_chunk_pool.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <library/cpp/iterator/concatenate.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NApi;
using namespace NChunkClient;
using namespace NChunkPools;
using namespace NConcurrency;
using namespace NControllerAgent::NProto;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NScheduler;
using namespace NTableClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TRemoteCopyController
    : public TOperationControllerBase
{
public:
    TRemoteCopyController(
        TRemoteCopyOperationSpecPtr spec,
        TControllerAgentConfigPtr config,
        TRemoteCopyOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation)
        : TOperationControllerBase(
            spec,
            config,
            options,
            host,
            operation)
        , Spec_(spec)
        , Options_(options)
        , Networks_(Spec_->Networks)
    {
        if (!Networks_ && Options_->Networks) {
            Networks_ = Options_->Networks;
        }

        if (!Spec_->AllowClusterConnection) {
            THROW_ERROR_EXCEPTION_IF(
                Spec_->ClusterConnection,
                "\"cluster_connection\" is not allowed in remote copy operation spec");

            THROW_ERROR_EXCEPTION_UNLESS(
                Spec_->ClusterName,
                "\"cluster_name\" is not set in remote copy operation spec");
        }
    }

protected:
    class TRemoteCopyTaskBase;
    using TRemoteCopyTaskBasePtr = TIntrusivePtr<TRemoteCopyTaskBase>;
    using TRemoteCopyTaskBaseWeakPtr = TWeakPtr<TRemoteCopyTaskBase>;

    class TRemoteCopyTaskBase
        : public TTask
    {
    public:
        //! For persistence only.
        TRemoteCopyTaskBase()
            : Controller_(nullptr)
            , IsInitializationCompleted_(false)
        { }

        TRemoteCopyTaskBase(TRemoteCopyController* controller)
            : TTask(controller, controller->GetStandardStreamDescriptors(), /*inputStreamDescriptors*/ {})
            , Controller_(controller)
            , IsInitializationCompleted_(false)
        {
            if (Controller_->Spec_->ClusterName && Controller_->Spec_->UseClusterThrottlers) {
                TClusterName clusterName{*Controller_->Spec_->ClusterName};
                UpdateClusterToNetworkBandwidthAvailability(
                    clusterName,
                    TaskHost_->IsNetworkBandwidthAvailable(clusterName));
                // Unsubscribe is done in destructor.
                SubscribeToClusterNetworkBandwidthAvailabilityUpdated(clusterName);
            }
        }

        void InitializeChunkPool()
        {
            auto options = Controller_->GetOrderedChunkPoolOptions(GetTitle());
            ChunkPool_ = CreateOrderedChunkPool(options, Controller_->GetInputStreamDirectory());
        }

        TDataFlowGraph::TVertexDescriptor GetVertexDescriptor() const override
        {
            return CamelCaseToUnderscoreCase(GetTitle());
        }

        IPersistentChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return ChunkPool_;
        }

        IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            return ChunkPool_;
        }

        IOrderedChunkPoolPtr GetOrderedChunkPool() const
        {
            return ChunkPool_;
        }

        void FinishInitialization()
        {
            IsInitializationCompleted_ = true;
        }

        void AddDependency(TRemoteCopyTaskBasePtr dependency)
        {
            YT_VERIFY(!IsInitializationCompleted_);
            dependency->AddDependent(MakeStrong(this));
            Dependencies_.emplace_back(std::move(dependency));
        }

    protected:
        TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobCompleted(joblet, jobSummary);

            TChunkStripeKey key(joblet->OutputCookie);
            RegisterOutput(jobSummary, joblet->ChunkListIds, joblet, key, /*processEmptyStripes*/ true);

            return result;
        }

    protected:
        TRemoteCopyController* Controller_;

        THashMap<TChunkId, TChunkId> GetResultHunkChunkIdMapping(const TJobResultExt& jobResultExt) const
        {
            THashMap<TChunkId, TChunkId> hunkChunkIdMapping;
            hunkChunkIdMapping.reserve(jobResultExt.hunk_chunk_id_mapping_size());
            for (const auto& mapping : jobResultExt.hunk_chunk_id_mapping()) {
                EmplaceOrCrash(
                    hunkChunkIdMapping,
                    FromProto<TChunkId>(mapping.input_hunk_chunk_id()),
                    FromProto<TChunkId>(mapping.output_hunk_chunk_id()));
            }
            return hunkChunkIdMapping;
        }

    private:
        IOrderedChunkPoolPtr ChunkPool_;

        // On which it depends.
        std::vector<TRemoteCopyTaskBaseWeakPtr> Dependencies_;
        // Which depends on it.
        std::vector<TRemoteCopyTaskBasePtr> Dependents_;
        bool IsInitializationCompleted_;

        TDuration GetLocalityTimeout() const override
        {
            return Controller_->IsLocalityEnabled()
                ? Controller_->Spec_->LocalityTimeout
                : TDuration::Zero();
        }

        TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
        {
            return GetMergeResources(joblet->InputStripeList->GetStatistics());
        }

        void BuildInputOutputJobSpec(TJobletPtr joblet, TJobSpec* jobSpec)
        {
            AddParallelInputSpec(jobSpec, joblet);
            AddOutputTableSpecs(jobSpec, joblet);
        }

        TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            return GetMergeResources(ChunkPool_->GetApproximateStripeStatistics());
        }

        TExtendedJobResources GetMergeResources(
            const TChunkStripeStatisticsVector& statistics) const
        {
            TExtendedJobResources result;
            result.SetUserSlots(1);
            result.SetCpu(Controller_->GetCpuLimit());

            auto jobProxyMemory = Controller_->GetFinalIOMemorySize(
                Controller_->Spec_->JobIO,
                /*useEstimatedBufferSize*/ true,
                statistics);
            auto jobProxyMemoryWithFixedWriteBufferSize = Controller_->GetFinalIOMemorySize(
                Controller_->Spec_->JobIO,
                /*useEstimatedBufferSize*/ false,
                statistics);

            result.SetJobProxyMemory(jobProxyMemory);
            result.SetJobProxyMemoryWithFixedWriteBufferSize(jobProxyMemoryWithFixedWriteBufferSize);

            AddFootprintAndUserJobResources(result);
            return result;
        }

        EJobType GetJobType() const override
        {
            return EJobType::RemoteCopy;
        }

        TUserJobSpecPtr GetUserJobSpec() const override
        {
            return nullptr;
        }

        void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller_->JobSpecTemplate_);
            BuildInputOutputJobSpec(joblet, jobSpec);
        }

        TJobSplitterConfigPtr GetJobSplitterConfig() const override
        {
            auto config = TaskHost_->GetJobSplitterConfigTemplate();
            config->EnableJobSplitting = false;

            return config;
        }

        bool IsJobInterruptible() const override
        {
            return false;
        }

        void AddDependent(TRemoteCopyTaskBasePtr dependent)
        {
            YT_VERIFY(!IsInitializationCompleted_);
            Dependents_.emplace_back(std::move(dependent));
        }

        void RemoveDependency(TRemoteCopyTaskBaseWeakPtr dependency)
        {
            YT_VERIFY(IsInitializationCompleted_);
            auto dependencyIt = std::find(Dependencies_.begin(), Dependencies_.end(), dependency);
            YT_VERIFY(dependencyIt != Dependencies_.end());
            Dependencies_.erase(dependencyIt);

            UpdateState();
        }

        TCompositePendingJobCount GetPendingJobCount() const override
        {
            YT_VERIFY(IsInitializationCompleted_);

            if (std::ssize(Dependencies_) > 0) {
                return {};
            }
            return TTask::GetPendingJobCount();
        }

        void UpdateState()
        {
            if (Dependencies_.empty()) {
                Controller_->FillJobSpecHunkChunkIdMapping();
                Controller_->FillJobSpecCompressionDictionaryIdMapping();
            }

            Controller_->UpdateTask(this);
        }

        void RemoveDependents()
        {
            YT_VERIFY(IsInitializationCompleted_);
            for (auto& dependant : Dependents_) {
                dependant->RemoveDependency(MakeWeak(this));
            }
        }

        void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            ValidateAllDataHaveBeenCopied();
            RemoveDependents();
        }

        virtual void ValidateAllDataHaveBeenCopied()
        { }

        PHOENIX_DECLARE_POLYMORPHIC_TYPE(TRemoteCopyTaskBase, 0x5859dab3);
    };

    class TRemoteCopyTask
        : public TRemoteCopyTaskBase
    {
    public:
        TRemoteCopyTask()
            : TRemoteCopyTaskBase()
        { }

        TRemoteCopyTask(TRemoteCopyController* controller)
            : TRemoteCopyTaskBase(controller)
        { }

    private:

        PHOENIX_DECLARE_POLYMORPHIC_TYPE(TRemoteCopyTask, 0xaba78385);
    };

    using TRemoteCopyTaskPtr = TIntrusivePtr<TRemoteCopyTask>;

    class TRemoteCopyHunkTask
        : public TRemoteCopyTaskBase
    {
    public:
        TRemoteCopyHunkTask()
            : TRemoteCopyTaskBase()
        { }

        TRemoteCopyHunkTask(TRemoteCopyController* controller)
            : TRemoteCopyTaskBase(controller)
        { }

        TString GetTitle() const override
        {
            return "HunkRemoteCopy";
        }

        TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto& jobResultExt = jobSummary.GetJobResultExt();
            Controller_->UpdateHunkChunkIdMapping(GetResultHunkChunkIdMapping(jobResultExt));
            return TRemoteCopyTaskBase::OnJobCompleted(joblet, jobSummary);
        }

    private:
        void ValidateAllDataHaveBeenCopied() override
        {
            Controller_->ValidateHunkChunksConsistency();
        }

        PHOENIX_DECLARE_POLYMORPHIC_TYPE(TRemoteCopyHunkTask, 0xaba78386);
    };

    class TRemoteCopyCompressionDictionaryTask
        : public TRemoteCopyTaskBase
    {
    public:
        TRemoteCopyCompressionDictionaryTask()
            : TRemoteCopyTaskBase()
        { }

        TRemoteCopyCompressionDictionaryTask(TRemoteCopyController* controller)
            : TRemoteCopyTaskBase(controller)
        { }

        TString GetTitle() const override
        {
            return "CompressionDictionaryRemoteCopy";
        }

        TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto& jobResultExt = jobSummary.GetJobResultExt();
            Controller_->UpdateCompressionDictionaryIdMapping(GetResultHunkChunkIdMapping(jobResultExt));
            return TRemoteCopyTaskBase::OnJobCompleted(joblet, jobSummary);
        }

    private:
        void ValidateAllDataHaveBeenCopied() override
        {
            Controller_->ValidateCompressionDictionariesConsistency();
        }

        PHOENIX_DECLARE_POLYMORPHIC_TYPE(TRemoteCopyCompressionDictionaryTask, 0xaba78387);
    };

    using TRemoteCopyHunkTaskPtr = TIntrusivePtr<TRemoteCopyHunkTask>;
    using TRemoteCopyCompressionDictionaryTaskPtr = TIntrusivePtr<TRemoteCopyCompressionDictionaryTask>;

private:
    TRemoteCopyOperationSpecPtr Spec_;
    TRemoteCopyOperationOptionsPtr Options_;

    //! Customized job IO config.
    TJobIOConfigPtr JobIOConfig_;

    //! The template for starting new jobs.
    TJobSpec JobSpecTemplate_;

    IJobSizeConstraintsPtr JobSizeConstraints_;

    i64 InputSliceDataWeight_;

    std::optional<NNodeTrackerClient::TNetworkPreferenceList> Networks_;

    IAttributeDictionaryPtr InputTableAttributes_;

    THashMap<TChunkId, TChunkId> HunkChunkIdMapping_;
    THashMap<TChunkId, TChunkId> CompressionDictionaryIdMapping_;
    THashSet<TChunkId> CompressionDictionaryIds_;
    THashSet<TChunkId> HunkChunkIds_;

    TRemoteCopyTaskPtr MainTask_;
    TRemoteCopyHunkTaskPtr HunkTask_;
    TRemoteCopyCompressionDictionaryTaskPtr CompressionDictionaryTask_;

    bool IsCompleted() const override
    {
        return MainTask_ &&
            MainTask_->IsCompleted() &&
            (!HunkTask_ || HunkTask_->IsCompleted()) &&
            (!CompressionDictionaryTask_ || CompressionDictionaryTask_->IsCompleted());
    }

    void ValidateTableType(const auto& table) const
    {
        if (table->Type != EObjectType::Table && table->Type != EObjectType::File) {
            THROW_ERROR_EXCEPTION("Only files and tables are allowed, but %v has type %Qlv",
                table->GetPath(),
                table->Type);
        }
    }

    void ValidateInputTablesTypes() const override
    {
        // NB(coteeq): remote_copy always has one input table.
        YT_VERIFY(InputManager_->GetInputTables().size() == 1);
        ValidateTableType(InputManager_->GetInputTables()[0]);
    }

    void ValidateUpdatingTablesTypes() const override
    {
        // NB(coteeq): remote_copy always has one input table.
        YT_VERIFY(OutputTables_.size() == 1);
        ValidateTableType(OutputTables_[0]);

        const auto& inputTables = InputManager_->GetInputTables();

        THROW_ERROR_EXCEPTION_UNLESS(
            OutputTables_[0]->Type == inputTables[0]->Type,
            "Output object type does not match that of the input object %v: expected %Qlv, found %Qlv",
            inputTables[0]->GetPath(),
            inputTables[0]->Type,
            OutputTables_[0]->Type);

        YT_VERIFY(!StderrTable_ && !CoreTable_);
    }

    EObjectType GetOutputTableDesiredType() const override
    {
        YT_VERIFY(InputManager_->GetInputTables().size() == 1);
        return InputManager_->GetInputTables()[0]->Type;
    }

    TStringBuf GetDataWeightParameterNameForJob(EJobType /*jobType*/) const override
    {
        YT_ABORT();
    }

    std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {};
    }

    bool ShouldVerifySortedOutput() const override
    {
        return false;
    }

    void BuildBriefSpec(TFluentMap fluent) const override
    {
        TOperationControllerBase::BuildBriefSpec(fluent);
        fluent
            .Item("cluster_name").Value(Spec_->ClusterName)
            .Item("networks").Value(Networks_);
    }

    // Custom bits of preparation pipeline.
    TTransactionId GetInputTransactionParentId() override
    {
        return {};
    }

    void InitializeClients() override
    {
        TOperationControllerBase::InitializeClients();

        InputClient_ = GetRemoteConnection()->CreateNativeClient(TClientOptions::FromUser(AuthenticatedUser_));
        SchedulerInputClient_ = GetRemoteConnection()->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::SchedulerUserName));
        InputManager_->InitializeClients(InputClient_);
    }

    std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec_->InputTablePaths;
    }

    std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return {Spec_->OutputTablePath};
    }

    TDataFlowGraph::TVertexDescriptor GetOutputLivePreviewVertexDescriptor() const override
    {
        return MainTask_->GetVertexDescriptor();
    }

    void PrepareOutputTables() override
    {
        auto& table = OutputTables_[0];

        ValidateSchemaInferenceMode(Spec_->SchemaInferenceMode);

        switch (Spec_->SchemaInferenceMode) {
            case ESchemaInferenceMode::Auto:
                if (table->TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    InferSchemaFromInputOrdered();
                    break;
                }
                [[fallthrough]];

            case ESchemaInferenceMode::FromOutput:
                ValidateOutputSchemaOrdered();

                // Since remote copy doesn't unpack blocks and validate schema, we must ensure
                // that schemas are identical.
                for (const auto& inputTable : InputManager_->GetInputTables()) {
                    if (table->TableUploadOptions.SchemaMode == ETableSchemaMode::Strong &&
                        *inputTable->Schema->ToCanonical() != *table->TableUploadOptions.TableSchema->ToCanonical())
                    {
                        THROW_ERROR_EXCEPTION("Cannot make remote copy into table with \"strong\" schema since "
                            "input table schema differs from output table schema")
                            << TErrorAttribute("input_table_schema", inputTable->Schema)
                            << TErrorAttribute("output_table_schema", *table->TableUploadOptions.TableSchema);
                    }
                }
                break;

            case ESchemaInferenceMode::FromInput:
                InferSchemaFromInputOrdered();
                break;
        }
    }

    void ValidateInputDataSlice(const TLegacyDataSlicePtr& dataSlice)
    {
        auto errorCode = NChunkClient::EErrorCode::InvalidInputChunk;
        if (!dataSlice->IsTrivial()) {
            THROW_ERROR_EXCEPTION(errorCode, "Remote copy operation does not support versioned data slices");
        }

        const auto& chunk = dataSlice->GetSingleUnversionedChunk();
        YT_VERIFY(!chunk->IsDynamicStore());
        if ((chunk->LowerLimit() && !IsTrivial(*chunk->LowerLimit())) ||
            (chunk->UpperLimit() && !IsTrivial(*chunk->UpperLimit())))
        {
            THROW_ERROR_EXCEPTION(
                errorCode,
                "Remote copy operation does not support non-trivial table limits%v",
                MakeFormatterWrapper([&] (auto* builder) {
                    if (InputManager_->GetInputTables()[0]->Dynamic) {
                        FormatValue(builder, " and chunks crossing tablet boundaries", "v");
                    }
                }));
        }
    }

    void CustomPrepare() override
    {
        TOperationControllerBase::CustomPrepare();

        static const auto allowedAttributes = [] {
            const auto& wellKnown = GetWellKnownRichYPathAttributes();
            return THashSet<TString>(wellKnown.begin(), wellKnown.end());
        }();

        for (const auto& attributeName : Spec_->OutputTablePath.Attributes().ListKeys()) {
            if (!allowedAttributes.contains(attributeName)) {
                THROW_ERROR_EXCEPTION("Found unexpected attribute %Qv in Rich YPath", attributeName)
                    << TErrorAttribute("path", Spec_->OutputTablePath);
            }
        }
    }

    std::vector<TString> BuildSystemAttributeKeys() const
    {
        if (!Spec_->ForceCopySystemAttributes) {
            return {};
        }

        std::vector<TString> keys{
            "compression_codec",
            "erasure_codec",
        };

        if (!InputManager_->GetInputTables()[0]->IsFile()) {
            keys.push_back("optimize_for");
        }

        return keys;
    }

    void CalculateSizes()
    {
        JobSizeConstraints_ = CreateRemoteCopyJobSizeConstraints(
            Spec_,
            Options_,
            Logger,
            EstimatedInputStatistics_->ChunkCount,
            EstimatedInputStatistics_->PrimaryDataWeight,
            EstimatedInputStatistics_->PrimaryCompressedDataSize,
            EstimatedInputStatistics_->DataWeightRatio,
            EstimatedInputStatistics_->CompressionRatio);

        InputSliceDataWeight_ = JobSizeConstraints_->GetInputSliceDataWeight();

        YT_LOG_INFO("Calculated operation parameters (JobCount: %v, MaxDataWeightPerJob: %v, InputSliceDataWeight: %v)",
            JobSizeConstraints_->GetJobCount(),
            JobSizeConstraints_->GetMaxDataWeightPerJob(),
            InputSliceDataWeight_);
    }

    void FetchInputTableAttributes()
    {
        if (!Spec_->ForceCopySystemAttributes && !Spec_->CopyAttributes) {
            return;
        }

        auto attributesFuture = InputManager_->FetchSingleInputTableAttributes(
            Spec_->CopyAttributes
                ? std::nullopt
                : std::make_optional(BuildSystemAttributeKeys()));

        InputTableAttributes_ = WaitFor(attributesFuture)
            .ValueOrThrow();
    }

    bool IsOrderedOutputRequired() const override
    {
        return true;
    }

    std::vector<TChunkTreeId> GetOutputChunkTreesInOrder(const TOutputTablePtr& table) const override
    {
        std::vector<std::pair<TOutputCookie, TChunkTreeId>> cookieAndTreeIdList;
        cookieAndTreeIdList.reserve(table->OutputChunkTreeIds.size());
        for (const auto& [key, treeId] : table->OutputChunkTreeIds) {
            YT_VERIFY(key.IsOutputCookie());
            cookieAndTreeIdList.emplace_back(key.AsOutputCookie(), treeId);
        }

        return MainTask_->GetOrderedChunkPool()->ArrangeOutputChunkTrees(cookieAndTreeIdList);
    }

    void CustomMaterialize() override
    {
        FetchInputTableAttributes();

        bool hasDynamicInputTable = false;
        bool hasDynamicOutputTable = false;
        bool hasStaticTableWithHunkChunks = false;
        for (const auto& table : InputManager_->GetInputTables()) {
            hasDynamicInputTable |= table->Dynamic;
            hasStaticTableWithHunkChunks |= !table->Dynamic && !table->HunkChunks.empty();
        }
        for (const auto& table : OutputTables_) {
            hasDynamicOutputTable |= table->Dynamic;
        }

        if (hasStaticTableWithHunkChunks) {
            THROW_ERROR_EXCEPTION("Static table with hunk chunks cannot be copied");
        }

        if (hasDynamicInputTable) {
            if (!hasDynamicOutputTable) {
                THROW_ERROR_EXCEPTION("Dynamic table can be copied only to another dynamic table");
            }
            if (InputManager_->GetInputTables().size() != 1 || OutputTables_.size() != 1) {
                THROW_ERROR_EXCEPTION("Only one dynamic table can be copied at a time");
            }
            if (OutputTables_[0]->TableUploadOptions.UpdateMode != EUpdateMode::Overwrite) {
                THROW_ERROR_EXCEPTION("Remote copy of dynamic tables can only be done in overwrite mode");
            }
        } else if (hasDynamicOutputTable) {
            THROW_ERROR_EXCEPTION("Static table cannot be copied into a dynamic table");
        }

        if (InputTableAttributes_) {
            YT_VERIFY(Spec_->CopyAttributes || Spec_->ForceCopySystemAttributes);
        }

        CalculateSizes();

        CreateTasks();

        RegisterTask(MainTask_);

        if (HunkTask_) {
            RegisterTask(HunkTask_);
        }

        if (CompressionDictionaryTask_) {
            RegisterTask(CompressionDictionaryTask_);
        }

        ProcessInputs();

        FinishTaskInput(MainTask_);

        if (HunkTask_) {
            FinishTaskInput(HunkTask_);
        }

        if (CompressionDictionaryTask_) {
            FinishTaskInput(CompressionDictionaryTask_);
        }

        FinishPreparation();
    }

    template <class TTask>
    TIntrusivePtr<TTask> CreateTask()
    {
        auto task = New<TTask>(this);
        task->InitializeChunkPool();
        return task;
    }

    void CreateTasks()
    {
        for (const auto& table : InputManager_->GetInputTables()) {
            CollectHunkChunkIdsByType(table, GetPtr(HunkChunkIds_), GetPtr(CompressionDictionaryIds_));
        }

        MainTask_ = CreateTask<TRemoteCopyTask>();
        if (!HunkChunkIds_.empty() || !CompressionDictionaryIds_.empty()) {
            HunkTask_ = CreateTask<TRemoteCopyHunkTask>();
            MainTask_->AddDependency(HunkTask_);
        }

        if (!CompressionDictionaryIds_.empty()) {
            CompressionDictionaryTask_ = CreateTask<TRemoteCopyCompressionDictionaryTask>();

            YT_VERIFY(HunkTask_);
            HunkTask_->AddDependency(CompressionDictionaryTask_);
            MainTask_->AddDependency(CompressionDictionaryTask_);
            CompressionDictionaryTask_->FinishInitialization();
        }

        if (HunkTask_) {
            HunkTask_->FinishInitialization();
        }

        MainTask_->FinishInitialization();
    }

    TOrderedChunkPoolOptions GetOrderedChunkPoolOptions(const std::string& name)
    {
        TOrderedChunkPoolOptions chunkPoolOptions;
        chunkPoolOptions.MaxTotalSliceCount = Config_->MaxTotalSliceCount;
        chunkPoolOptions.EnablePeriodicYielder = true;
        chunkPoolOptions.MinTeleportChunkSize = std::numeric_limits<i64>::max() / 4;
        chunkPoolOptions.JobSizeConstraints = JobSizeConstraints_;
        chunkPoolOptions.ShouldSliceByRowIndices = false;
        chunkPoolOptions.UseNewSlicingImplementation = GetSpec()->UseNewSlicingImplementationInOrderedPool;
        chunkPoolOptions.Logger = Logger().WithTag("Name: %v", name);
        return chunkPoolOptions;
    }

    TChunkStripePtr CreateChunkStripe(TLegacyDataSlicePtr dataSlice)
    {
        TChunkStripePtr chunkStripe = New<TChunkStripe>(false /*foreign*/);
        chunkStripe->DataSlices.emplace_back(std::move(dataSlice));
        return chunkStripe;
    }

    int GetTaskCount() const
    {
        int count = 0;
        if (MainTask_) {
            ++count;
        }
        if (HunkTask_) {
            ++count;
        }
        return count;
    }

    int AddInputSlices()
    {
        TPeriodicYielder yielder(PrepareYieldPeriod);

        std::vector<TLegacyDataSlicePtr> hunkChunkSlices;
        std::vector<TLegacyDataSlicePtr> compressionDictionarySlices;
        std::vector<TLegacyDataSlicePtr> chunkSlices;

        for (const auto& chunk : Concatenate(InputManager_->CollectPrimaryUnversionedChunks(), InputManager_->CollectPrimaryVersionedChunks())) {
            auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
            dataSlice->SetInputStreamIndex(InputStreamDirectory_.GetInputStreamIndex(chunk->GetTableIndex(), chunk->GetRangeIndex()));

            const auto& inputTable = InputManager_->GetInputTables()[dataSlice->GetTableIndex()];
            dataSlice->TransformToNew(RowBuffer_, inputTable->Comparator);

            ValidateInputDataSlice(dataSlice);
            if (chunk->IsHunk()) {
                // There might be hunk chunks that are not referenced by any chunks, so we don't need to copy them.
                if (CompressionDictionaryIds_.contains(chunk->GetChunkId())) {
                    compressionDictionarySlices.emplace_back(std::move(dataSlice));
                } else if (HunkChunkIds_.contains(chunk->GetChunkId())) {
                    hunkChunkSlices.emplace_back(std::move(dataSlice));
                }
            } else {
                chunkSlices.emplace_back(std::move(dataSlice));
            }
        }

        YT_VERIFY(std::ssize(HunkChunkIds_) == std::ssize(hunkChunkSlices));
        YT_VERIFY(std::ssize(CompressionDictionaryIds_) == std::ssize(compressionDictionarySlices));

        int sliceCount = 0;
        auto addInputSlices = [&] (const TRemoteCopyTaskBasePtr& task, std::vector<TLegacyDataSlicePtr>&& slices) {
            sliceCount += std::ssize(slices);
            for (auto& slice : slices) {
                task->AddInput(CreateChunkStripe(std::move(slice)));
                yielder.TryYield();
            }
        };

        addInputSlices(MainTask_, std::move(chunkSlices));

        if (HunkTask_) {
            addInputSlices(HunkTask_, std::move(hunkChunkSlices));
        } else {
            YT_VERIFY(hunkChunkSlices.empty());
        }

        if (CompressionDictionaryTask_) {
            addInputSlices(CompressionDictionaryTask_, std::move(compressionDictionarySlices));
        } else {
            YT_VERIFY(compressionDictionarySlices.empty());
        }

        return sliceCount;
    }

    void ProcessInputs()
    {
        YT_PROFILE_TIMING("/operations/remote_copy/input_processing_time") {
            YT_LOG_INFO("Processing inputs");

            MainTask_->SetIsInput(true);
            if (HunkTask_) {
                HunkTask_->SetIsInput(true);
            }
            if (CompressionDictionaryTask_) {
                CompressionDictionaryTask_->SetIsInput(true);
            }

            auto sliceCount = AddInputSlices();
            YT_LOG_INFO("Processed inputs (Slices: %v)", sliceCount);
        }
    }

    void FinishPreparation()
    {
        JobIOConfig_ = CloneYsonStruct(Spec_->JobIO);
        InitJobSpecTemplate();
    }

    bool HasHunkChunks() const
    {
        return HunkTask_ != nullptr;
    }

    std::vector<TString> BuildOutputTableAttributeKeys() const
    {
        YT_VERIFY(Spec_->CopyAttributes || Spec_->ForceCopySystemAttributes);

        auto systemAttributeKeys = BuildSystemAttributeKeys();
        auto attributeKeys = systemAttributeKeys;
        if (Spec_->CopyAttributes) {
            // Filter out unneeded attributes.
            auto userAttributeKeys = InputTableAttributes_->Get<std::vector<TString>>("user_attribute_keys");
            auto specAttributeKeys = Spec_->AttributeKeys.value_or(userAttributeKeys);
            attributeKeys.reserve(attributeKeys.size() + specAttributeKeys.size());
            for (const auto& key : specAttributeKeys) {
                auto isSystemAttribute = [&] (const auto& key) {
                    return std::find(
                        systemAttributeKeys.begin(),
                        systemAttributeKeys.end(),
                        key) != systemAttributeKeys.end();
                };

                if (isSystemAttribute(key)) {
                    // Do not duplicate system attributes' keys.
                    continue;
                }
                attributeKeys.push_back(key);
            }
        }

        return attributeKeys;
    }

    void CustomCommit() override
    {
        TOperationControllerBase::CustomCommit();

        // COMPAT(coteeq)
        if (InputTableAttributes_) {
            const auto& path = Spec_->OutputTablePath.GetPath();

            auto proxy = CreateObjectServiceWriteProxy(OutputClient_);

            auto attributeKeys = BuildOutputTableAttributeKeys();
            auto batchReq = proxy.ExecuteBatch();
            auto req = TYPathProxy::MultisetAttributes(path + "/@");
            SetTransactionId(req, OutputCompletionTransaction_->GetId());

            for (const auto& attribute : attributeKeys) {
                auto* subrequest = req->add_subrequests();
                subrequest->set_attribute(ToYPathLiteral(attribute));
                auto value = InputTableAttributes_->GetYson(attribute);
                ValidateYson(value, GetYsonNestingLevelLimit());
                subrequest->set_value(ToProto(value));
            }

            batchReq->AddRequest(req);

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error setting attributes for output table %v",
                path);
        }
    }

    void InitJobSpecTemplate()
    {
        JobSpecTemplate_.set_type(ToProto(EJobType::RemoteCopy));
        auto* jobSpecExt = JobSpecTemplate_.MutableExtension(
            TJobSpecExt::job_spec_ext);

        jobSpecExt->set_io_config(ToProto(ConvertToYsonString(JobIOConfig_)));
        jobSpecExt->set_table_reader_options("");
        SetProtoExtension<NChunkClient::NProto::TDataSourceDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            BuildDataSourceDirectoryFromInputTables(InputManager_->GetInputTables()));
        SetProtoExtension<NChunkClient::NProto::TDataSinkDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            BuildDataSinkDirectoryFromOutputTables(OutputTables_));

        auto connectionConfig = GetRemoteConnectionConfig()->Clone();
        if (Networks_) {
            connectionConfig->Static->Networks = *Networks_;
        }

        auto masterCacheAddresses = GetRemoteMasterCacheAddresses();
        if (masterCacheAddresses.empty()) {
            YT_LOG_DEBUG("Not using remote master caches for remote copy operation");
        } else {
            connectionConfig->Static->OverrideMasterAddresses(masterCacheAddresses);

            YT_LOG_DEBUG("Using remote master caches for remote copy operation (Addresses: %v)",
                masterCacheAddresses);
        }

        auto* remoteCopyJobSpecExt = JobSpecTemplate_.MutableExtension(TRemoteCopyJobSpecExt::remote_copy_job_spec_ext);
        auto connectionNode = ConvertToNode(connectionConfig)->AsMap();
        remoteCopyJobSpecExt->set_connection_config(ToProto(ConvertToYsonString(connectionNode)));
        remoteCopyJobSpecExt->set_concurrency(Spec_->Concurrency);
        remoteCopyJobSpecExt->set_block_buffer_size(Spec_->BlockBufferSize);
        remoteCopyJobSpecExt->set_delay_in_copy_chunk(ToProto(Spec_->DelayInCopyChunk));
        remoteCopyJobSpecExt->set_erasure_chunk_repair_delay(ToProto(Spec_->ErasureChunkRepairDelay));
        remoteCopyJobSpecExt->set_repair_erasure_chunks(Spec_->RepairErasureChunks);

        // TODO(yuryalekseev): Prohibit ClusterConnection in Spec_.
        if (Spec_->ClusterName) {
            remoteCopyJobSpecExt->set_remote_cluster_name(*Spec_->ClusterName);
        }
    }

    TError GetUseChunkSliceStatisticsError() const override
    {
        return TError();
    }

    NNative::IConnectionPtr GetRemoteConnection() const
    {
        if (Spec_->ClusterConnection) {
            NNative::TConnectionOptions connectionOptions;
            connectionOptions.ConnectionInvoker = Host_->GetConnectionInvoker();
            return NApi::NNative::CreateConnection(
                Spec_->ClusterConnection,
                std::move(connectionOptions));
        } else if (Spec_->ClusterName) {
            return Host_
                ->GetClient()
                ->GetNativeConnection()
                ->GetClusterDirectory()
                ->GetConnectionOrThrow(*Spec_->ClusterName);
        } else {
            THROW_ERROR_EXCEPTION("No remote cluster is specified");
        }
    }

    NApi::NNative::TConnectionCompoundConfigPtr GetRemoteConnectionConfig() const
    {
        return GetRemoteConnection()->GetCompoundConfig();
    }

    std::vector<std::string> GetRemoteMasterCacheAddresses() const
    {
        try {
            return GuardedGetRemoteMasterCacheAddresses();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to get remote master cache addresses")
                << ex;
        }
    }

    std::vector<std::string> GuardedGetRemoteMasterCacheAddresses() const
    {
        if (!Spec_->UseRemoteMasterCaches) {
            return {};
        }

        TGetClusterMetaOptions options{
            .PopulateMasterCacheNodeAddresses = true,
        };
        auto clusterMeta = WaitFor(InputClient_->GetClusterMeta(options))
            .ValueOrThrow();
        return clusterMeta.MasterCacheNodeAddresses;
    }

    EChunkAvailabilityPolicy GetChunkAvailabilityPolicy() const override
    {
        // If repair in remote copy is disabled, all parts are required.
        if (!Spec_->RepairErasureChunks) {
            return EChunkAvailabilityPolicy::AllPartsAvailable;
        }

        return Spec_->ChunkAvailabilityPolicy;
    }

    TYsonStructPtr GetTypedSpec() const override
    {
        return Spec_;
    }

    TOperationSpecBasePtr ParseTypedSpec(const INodePtr& spec) const override
    {
        return ParseOperationSpec<TRemoteCopyOperationSpec>(spec);
    }

    TOperationSpecBaseConfigurator GetOperationSpecBaseConfigurator() const override
    {
        return TConfigurator<TRemoteCopyOperationSpec>();
    }

    TCpuResource GetCpuLimit() const
    {
        return Options_->CpuLimit;
    }

    void UpdateHunkChunkIdMapping(const THashMap<TChunkId, TChunkId>& newMapping)
    {
        for (const auto& mapping : newMapping) {
            EmplaceOrCrash(
                HunkChunkIdMapping_,
                mapping.first,
                mapping.second);
        }
    }

    void UpdateCompressionDictionaryIdMapping(const THashMap<TGuid, TGuid>& newMapping)
    {
        for (const auto& mapping : newMapping) {
            EmplaceOrCrash(
                CompressionDictionaryIdMapping_,
                mapping.first,
                mapping.second);
        }
    }

    void ValidateConsistency(const THashMap<TChunkId, TChunkId>& mapping, const THashSet<TChunkId>& chunkIds, const TStringBuf& chunkName) const
    {
        if (mapping.size() != chunkIds.size()) {
            for (const auto& compressionDictionaryId : CompressionDictionaryIds_) {
                YT_LOG_FATAL_IF(!CompressionDictionaryIdMapping_.contains(compressionDictionaryId),
                    "Validate %v consistency failed. Chunk %v was not copied",
                    chunkName,
                    compressionDictionaryId);
            }
            for (const auto& [oldId, newId] : mapping) {
                YT_LOG_FATAL_IF(!chunkIds.contains(oldId),
                    "Validate %v consistency failed. Chunk %v should not have been copied as %v",
                    chunkName,
                    oldId,
                    newId);
            }
        }
    }

    void ValidateHunkChunksConsistency() const
    {
        static const TStringBuf chunkName = "hunk chunks";
        ValidateConsistency(HunkChunkIdMapping_, HunkChunkIds_, chunkName);

    }

    void ValidateCompressionDictionariesConsistency() const
    {
        static const TStringBuf chunkName = "compression dictionaries";
        ValidateConsistency(CompressionDictionaryIdMapping_, CompressionDictionaryIds_, chunkName);
    }

    void FillJobSpecHunkChunkIdMapping()
    {
        auto* remoteCopyJobSpecExt = JobSpecTemplate_.MutableExtension(TRemoteCopyJobSpecExt::remote_copy_job_spec_ext);
        if (remoteCopyJobSpecExt->hunk_chunk_id_mapping_size() > 0) {
            // We should fill mapping exactly once.
            return;
        }

        for (const auto& mapping : HunkChunkIdMapping_) {
            auto* protoMapping = remoteCopyJobSpecExt->add_hunk_chunk_id_mapping();
            ToProto(protoMapping->mutable_input_hunk_chunk_id(), mapping.first);
            ToProto(protoMapping->mutable_output_hunk_chunk_id(), mapping.second);
        }
    }

    void FillJobSpecCompressionDictionaryIdMapping()
    {
        auto* remoteCopyJobSpecExt = JobSpecTemplate_.MutableExtension(TRemoteCopyJobSpecExt::remote_copy_job_spec_ext);
        if (remoteCopyJobSpecExt->compression_dictionary_id_mapping_size() > 0) {
            // We should fill mapping exactly once.
            return;
        }

        for (const auto& mapping : CompressionDictionaryIdMapping_) {
            auto* protoMapping = remoteCopyJobSpecExt->add_compression_dictionary_id_mapping();
            ToProto(protoMapping->mutable_input_hunk_chunk_id(), mapping.first);
            ToProto(protoMapping->mutable_output_hunk_chunk_id(), mapping.second);
        }
    }

    void CollectHunkChunkIdsByType(
        const TInputTablePtr& table,
        TNonNullPtr<THashSet<TChunkId>> hunkChunkIds,
        TNonNullPtr<THashSet<TChunkId>> compressionDictionaryIds) const
    {
        if (table->HunkChunks.empty()) {
            return;
        }

        for (const auto& chunk : table->Chunks) {
            if (chunk->CompressionDictionaryId()) {
                compressionDictionaryIds->insert(*chunk->CompressionDictionaryId());
            }

            if (!chunk->HunkChunkRefsExt()) {
                continue;
            }

            for (const auto& hunkChunkRef : chunk->HunkChunkRefsExt()->refs()) {
                hunkChunkIds->insert(FromProto<TChunkId>(hunkChunkRef.chunk_id()));
                if (hunkChunkRef.has_compression_dictionary_id()) {
                    compressionDictionaryIds->insert(FromProto<TChunkId>(hunkChunkRef.compression_dictionary_id()));
                }
            }
        }

        for (auto compressionDictionaryId : *compressionDictionaryIds) {
            hunkChunkIds->erase(compressionDictionaryId);
        }
    }

    PHOENIX_DECLARE_FRIEND();

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TRemoteCopyController, 0xaa8829a9);
};

void TRemoteCopyController::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TOperationControllerBase>();

    PHOENIX_REGISTER_FIELD(1, Spec_);
    PHOENIX_REGISTER_FIELD(2, Options_);
    PHOENIX_REGISTER_FIELD(3, InputTableAttributes_,
        .template Serializer<TAttributeDictionarySerializer>());
    PHOENIX_REGISTER_FIELD(4, Networks_);
    PHOENIX_REGISTER_FIELD(5, JobIOConfig_);
    PHOENIX_REGISTER_FIELD(6, JobSpecTemplate_);
    PHOENIX_REGISTER_FIELD(7, JobSizeConstraints_);
    PHOENIX_REGISTER_FIELD(8, InputSliceDataWeight_);
    PHOENIX_REGISTER_FIELD(9, MainTask_);
    PHOENIX_REGISTER_FIELD(10, HunkTask_);
    PHOENIX_REGISTER_FIELD(11, CompressionDictionaryTask_);
    PHOENIX_REGISTER_FIELD(12, HunkChunkIdMapping_);
    PHOENIX_REGISTER_FIELD(13, CompressionDictionaryIdMapping_);
    PHOENIX_REGISTER_FIELD(14, CompressionDictionaryIds_);
    PHOENIX_REGISTER_FIELD(15, HunkChunkIds_);

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        if (this_->InputTableAttributes_ && this_->InputTableAttributes_->ListKeys().empty()) {
            this_->InputTableAttributes_.Reset();
        }
    });
}

PHOENIX_DEFINE_TYPE(TRemoteCopyController);

////////////////////////////////////////////////////////////////////////////////

void TRemoteCopyController::TRemoteCopyTaskBase::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TTask>();

    PHOENIX_REGISTER_FIELD(1, Controller_);
    PHOENIX_REGISTER_FIELD(2, ChunkPool_);
    PHOENIX_REGISTER_FIELD(3, Dependencies_);
    PHOENIX_REGISTER_FIELD(4, Dependents_);
    PHOENIX_REGISTER_FIELD(5, IsInitializationCompleted_);
}

PHOENIX_DEFINE_TYPE(TRemoteCopyController::TRemoteCopyTaskBase);

////////////////////////////////////////////////////////////////////////////////

void TRemoteCopyController::TRemoteCopyTask::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TRemoteCopyTaskBase>();
}

PHOENIX_DEFINE_TYPE(TRemoteCopyController::TRemoteCopyTask);

////////////////////////////////////////////////////////////////////////////////

void TRemoteCopyController::TRemoteCopyHunkTask::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TRemoteCopyTaskBase>();
}

PHOENIX_DEFINE_TYPE(TRemoteCopyController::TRemoteCopyHunkTask);

////////////////////////////////////////////////////////////////////////////////

void TRemoteCopyController::TRemoteCopyCompressionDictionaryTask::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TRemoteCopyTaskBase>();
}

PHOENIX_DEFINE_TYPE(TRemoteCopyController::TRemoteCopyCompressionDictionaryTask);

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateRemoteCopyController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto options = CreateOperationOptions(config->RemoteCopyOperationOptions, operation->GetOptionsPatch());
    auto spec = ParseOperationSpec<TRemoteCopyOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
    return New<TRemoteCopyController>(spec, config, options, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
