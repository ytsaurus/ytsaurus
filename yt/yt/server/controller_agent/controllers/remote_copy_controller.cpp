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

#include <library/cpp/iterator/concatenate.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NApi;
using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NChunkClient;
using namespace NChunkPools;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NControllerAgent::NProto;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NScheduler;

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

            auto options = controller->GetOrderedChunkPoolOptions();
            ChunkPool_ = CreateOrderedChunkPool(options, controller->GetInputStreamDirectory());
        }

        IPersistentChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return ChunkPool_;
        }

        IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override
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

            TChunkStripeKey key = TOutputOrder::TEntry(joblet->OutputCookie);
            RegisterOutput(jobSummary, joblet->ChunkListIds, joblet, key, /*processEmptyStripes*/ true);

            return result;
        }

    protected:
        TRemoteCopyController* Controller_;

    private:
        IPersistentChunkPoolPtr ChunkPool_;

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
            Controller_->UpdateTask(this);

            if (Dependencies_.empty()) {
                Controller_->FillJobSpecHunkChunkIdMapping();
            }
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
            THashMap<TChunkId, TChunkId> newHunkChunkIdMapping;
            for (const auto& mapping : jobResultExt.hunk_chunk_id_mapping()) {
                EmplaceOrCrash(
                    newHunkChunkIdMapping,
                    FromProto<TChunkId>(mapping.input_hunk_chunk_id()),
                    FromProto<TChunkId>(mapping.output_hunk_chunk_id()));
            }
            Controller_->UpdateHunkChunkIdMapping(newHunkChunkIdMapping);

            return TRemoteCopyTaskBase::OnJobCompleted(joblet, jobSummary);
        }

    private:
        void ValidateAllDataHaveBeenCopied() override
        {
            Controller_->ValidateHunkChunksConsistency();
        }

        PHOENIX_DECLARE_POLYMORPHIC_TYPE(TRemoteCopyHunkTask, 0xaba78386);
    };

    using TRemoteCopyHunkTaskPtr = TIntrusivePtr<TRemoteCopyHunkTask>;

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

    TRemoteCopyTaskPtr MainTask_;
    TRemoteCopyHunkTaskPtr HunkTask_;

    bool IsCompleted() const override
    {
        return MainTask_ &&
            MainTask_->IsCompleted() &&
            (!HunkTask_ || HunkTask_->IsCompleted());
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
        YT_VERIFY(InputManager->GetInputTables().size() == 1);
        ValidateTableType(InputManager->GetInputTables()[0]);
    }

    void ValidateUpdatingTablesTypes() const override
    {
        // NB(coteeq): remote_copy always has one input table.
        YT_VERIFY(OutputTables_.size() == 1);
        ValidateTableType(OutputTables_[0]);

        const auto& inputTables = InputManager->GetInputTables();

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
        YT_VERIFY(InputManager->GetInputTables().size() == 1);
        return InputManager->GetInputTables()[0]->Type;
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

        InputClient = GetRemoteConnection()->CreateNativeClient(TClientOptions::FromUser(AuthenticatedUser));
        SchedulerInputClient = GetRemoteConnection()->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::SchedulerUserName));
        InputManager->InitializeClients(InputClient);
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
                for (const auto& inputTable : InputManager->GetInputTables()) {
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
                    if (InputManager->GetInputTables()[0]->Dynamic) {
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

        if (!InputManager->GetInputTables()[0]->IsFile()) {
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
            TotalEstimatedInputChunkCount,
            PrimaryInputDataWeight,
            PrimaryInputCompressedDataSize,
            DataWeightRatio,
            InputCompressionRatio);

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

        auto attributesFuture = InputManager->FetchSingleInputTableAttributes(
            Spec_->CopyAttributes
                ? std::nullopt
                : std::make_optional(BuildSystemAttributeKeys()));

        InputTableAttributes_ = WaitFor(attributesFuture)
            .ValueOrThrow();
    }

    TOutputOrderPtr GetOutputOrder() const override
    {
        return MainTask_->GetChunkPoolOutput()->GetOutputOrder();
    }

    void CustomMaterialize() override
    {
        FetchInputTableAttributes();

        bool hasDynamicInputTable = false;
        bool hasDynamicOutputTable = false;
        bool hasStaticTableWithHunkChunks = false;
        for (const auto& table : InputManager->GetInputTables()) {
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
            if (InputManager->GetInputTables().size() != 1 || OutputTables_.size() != 1) {
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

        ProcessInputs();

        FinishTaskInput(MainTask_);
        if (HunkTask_) {
            FinishTaskInput(HunkTask_);
        }

        FinishPreparation();
    }

    void CreateTasks()
    {
        bool hasDynamicTableWithHunkChunks = InputManager->HasDynamicTableWithHunkChunks();

        MainTask_ = New<TRemoteCopyTask>(this);
        if (hasDynamicTableWithHunkChunks) {
            HunkTask_ = New<TRemoteCopyHunkTask>(this);
            MainTask_->AddDependency(HunkTask_);
            HunkTask_->FinishInitialization();
        }

        MainTask_->FinishInitialization();
    }

    TOrderedChunkPoolOptions GetOrderedChunkPoolOptions()
    {
        TOrderedChunkPoolOptions chunkPoolOptions;
        chunkPoolOptions.MaxTotalSliceCount = Config->MaxTotalSliceCount;
        chunkPoolOptions.EnablePeriodicYielder = true;
        chunkPoolOptions.MinTeleportChunkSize = std::numeric_limits<i64>::max() / 4;
        chunkPoolOptions.JobSizeConstraints = JobSizeConstraints_;
        chunkPoolOptions.BuildOutputOrder = true;
        chunkPoolOptions.ShouldSliceByRowIndices = false;
        chunkPoolOptions.UseNewSlicingImplementation = GetSpec()->UseNewSlicingImplementationInOrderedPool;
        chunkPoolOptions.Logger = Logger().WithTag("Name: Root");
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
        std::vector<TLegacyDataSlicePtr> chunkSlices;

        for (const auto& chunk : Concatenate(InputManager->CollectPrimaryUnversionedChunks(), InputManager->CollectPrimaryVersionedChunks())) {
            auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
            dataSlice->SetInputStreamIndex(InputStreamDirectory_.GetInputStreamIndex(chunk->GetTableIndex(), chunk->GetRangeIndex()));

            const auto& inputTable = InputManager->GetInputTables()[dataSlice->GetTableIndex()];
            dataSlice->TransformToNew(RowBuffer, inputTable->Comparator);

            ValidateInputDataSlice(dataSlice);
            if (chunk->IsHunk()) {
                hunkChunkSlices.emplace_back(std::move(dataSlice));
            } else {
                chunkSlices.emplace_back(std::move(dataSlice));
            }
        }

        int sliceCount = 0;
        auto addInputSlices = [&] (const TRemoteCopyTaskBasePtr& task, std::vector<TLegacyDataSlicePtr>& slices) {
            sliceCount += std::ssize(slices);
            for (auto& slice : slices) {
                task->AddInput(CreateChunkStripe(std::move(slice)));
                yielder.TryYield();
            }
        };

        addInputSlices(MainTask_, chunkSlices);
        if (HunkTask_) {
            addInputSlices(HunkTask_, hunkChunkSlices);
        } else {
            YT_VERIFY(hunkChunkSlices.empty());
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

            auto proxy = CreateObjectServiceWriteProxy(OutputClient);

            auto attributeKeys = BuildOutputTableAttributeKeys();
            auto batchReq = proxy.ExecuteBatch();
            auto req = TYPathProxy::MultisetAttributes(path + "/@");
            SetTransactionId(req, OutputCompletionTransaction->GetId());

            for (const auto& attribute : attributeKeys) {
                auto* subrequest = req->add_subrequests();
                subrequest->set_attribute(ToYPathLiteral(attribute));
                auto value = InputTableAttributes_->GetYson(attribute);
                ValidateYson(value, GetYsonNestingLevelLimit());
                subrequest->set_value(value.ToString());
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

        jobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig_).ToString());
        jobSpecExt->set_table_reader_options("");
        SetProtoExtension<NChunkClient::NProto::TDataSourceDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            BuildDataSourceDirectoryFromInputTables(InputManager->GetInputTables()));
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
        remoteCopyJobSpecExt->set_connection_config(ConvertToYsonString(connectionNode).ToString());
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
            connectionOptions.ConnectionInvoker = Host->GetConnectionInvoker();
            return NApi::NNative::CreateConnection(
                Spec_->ClusterConnection,
                std::move(connectionOptions));
        } else if (Spec_->ClusterName) {
            return Host
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
        auto clusterMeta = WaitFor(InputClient->GetClusterMeta(options))
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

    void ValidateHunkChunksConsistency() const
    {
        YT_VERIFY(HunkChunkIdMapping_.size() == InputManager->GetInputTables()[0]->HunkChunks.size());
    }

    void FillJobSpecHunkChunkIdMapping()
    {
        auto* remoteCopyJobSpecExt = JobSpecTemplate_.MutableExtension(TRemoteCopyJobSpecExt::remote_copy_job_spec_ext);

        for (const auto& mapping : HunkChunkIdMapping_) {
            auto* protoMapping = remoteCopyJobSpecExt->add_hunk_chunk_id_mapping();
            ToProto(protoMapping->mutable_input_hunk_chunk_id(), mapping.first);
            ToProto(protoMapping->mutable_output_hunk_chunk_id(), mapping.second);
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
    PHOENIX_REGISTER_FIELD(5, HunkChunkIdMapping_);
    PHOENIX_REGISTER_FIELD(6, JobIOConfig_);
    PHOENIX_REGISTER_FIELD(7, JobSpecTemplate_);
    PHOENIX_REGISTER_FIELD(8, JobSizeConstraints_);
    PHOENIX_REGISTER_FIELD(9, InputSliceDataWeight_);
    PHOENIX_REGISTER_FIELD(10, MainTask_);
    PHOENIX_REGISTER_FIELD(11, HunkTask_);

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
