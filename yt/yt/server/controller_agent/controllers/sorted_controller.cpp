#include "sorted_controller.h"

#include "auto_merge_task.h"
#include "job_info.h"
#include "job_memory.h"
#include "helpers.h"
#include "operation_controller_detail.h"
#include "task.h"

#include <yt/yt/server/controller_agent/chunk_list_pool.h>
#include <yt/yt/server/controller_agent/helpers.h>
#include <yt/yt/server/controller_agent/job_size_constraints.h>
#include <yt/yt/server/controller_agent/operation.h>
#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/legacy_sorted_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/new_sorted_chunk_pool.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_scraper.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_slice_fetcher.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/check_schema_compatibility.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/misc/numeric_helpers.h>

#include <util/generic/cast.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NChunkClient;
using namespace NChunkPools;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NScheduler::NProto;
using namespace NChunkClient::NProto;
using namespace NJobTrackerClient;
using namespace NControllerAgent::NProto;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NScheduler;

using NYT::FromProto;
using NYT::ToProto;

using NTableClient::TLegacyKey;

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): support Config->MaxTotalSliceCount
// TODO(max42): reorder virtual methods in public section.

class TSortedControllerBase
    : public TOperationControllerBase
{
public:
    TSortedControllerBase(
        TSimpleOperationSpecBasePtr spec,
        TControllerAgentConfigPtr config,
        TSimpleOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation)
        : TOperationControllerBase(
            spec,
            std::move(config),
            options,
            std::move(host),
            operation)
        , Spec_(std::move(spec))
        , Options_(std::move(options))
    { }

    // Persistence.

    void Persist(const TPersistenceContext& context) override
    {
        TOperationControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, Spec_);
        Persist(context, Options_);
        Persist(context, JobIOConfig_);
        Persist(context, JobSpecTemplate_);
        Persist(context, JobSizeConstraints_);
        Persist(context, InputSliceDataWeight_);
        Persist(context, SortedTask_);
        Persist(context, PrimarySortColumns_);
        Persist(context, ForeignSortColumns_);
    }

protected:
    TSimpleOperationSpecBasePtr Spec_;
    TSimpleOperationOptionsPtr Options_;

    //! Customized job IO config.
    TJobIOConfigPtr JobIOConfig_;

    //! The template for starting new jobs.
    TJobSpec JobSpecTemplate_;

    class TSortedTaskBase
        : public TTask
    {
    public:
        //! For persistence only.
        TSortedTaskBase()
            : Controller_(nullptr)
        { }

        TSortedTaskBase(
            TSortedControllerBase* controller,
            std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
            std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors)
            : TTask(controller, std::move(outputStreamDescriptors), std::move(inputStreamDescriptors))
            , Controller_(controller)
            , Options_(controller->GetSortedChunkPoolOptions())
            , UseNewSortedPool_(ParseOperationSpec<TSortedOperationSpec>(ConvertToNode(Controller_->GetSpec()))->UseNewSortedPool)
        {
            if (UseNewSortedPool_) {
                YT_LOG_INFO("Operation uses new sorted pool");
                ChunkPool_ = CreateNewSortedChunkPool(
                    Options_,
                    controller->CreateChunkSliceFetcherFactory(),
                    controller->GetInputStreamDirectory());
            } else {
                YT_LOG_INFO("Operation uses legacy sorted pool");
                ChunkPool_ = CreateLegacySortedChunkPool(
                    Options_,
                    controller->CreateChunkSliceFetcherFactory(),
                    controller->GetInputStreamDirectory());
            }

            ChunkPool_->SubscribeChunkTeleported(BIND(&TSortedTaskBase::OnChunkTeleported, MakeWeak(this)));
        }

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

        IPersistentChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return ChunkPool_;
        }

        IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            return ChunkPool_;
        }

        void Persist(const TPersistenceContext& context) override
        {
            TTask::Persist(context);

            using NYT::Persist;
            Persist(context, Controller_);
            Persist(context, ChunkPool_);
            Persist(context, TotalOutputRowCount_);
            Persist(context, UseNewSortedPool_);

            ChunkPool_->SubscribeChunkTeleported(BIND(&TSortedTaskBase::OnChunkTeleported, MakeWeak(this)));
        }

        void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            if (Controller_->AutoMergeTask_) {
                Controller_->AutoMergeTask_->FinishInput();
            }
        }

        i64 GetTotalOutputRowCount() const
        {
            return TotalOutputRowCount_;
        }

        void AdjustDataSliceForPool(const TLegacyDataSlicePtr& dataSlice) const override
        {
            // Consider the following case as an example: we are running reduce over a sorted table
            // with two key columns [key, subkey] and a range >[5, foo]:<=[7, bar]. From the
            // pool's point of this slice covers a range >=[5]:<=[7] (note that boundaries become inclusive
            // when truncated).

            YT_VERIFY(!dataSlice->IsLegacy);

            auto inputStreamDescriptor = Controller_->GetInputStreamDirectory().GetDescriptor(dataSlice->GetInputStreamIndex());

            auto prefixLength = inputStreamDescriptor.IsPrimary()
                ? Controller_->PrimarySortColumns_.size()
                : Controller_->ForeignSortColumns_.size();

            dataSlice->LowerLimit().KeyBound = ShortenKeyBound(dataSlice->LowerLimit().KeyBound, prefixLength, TaskHost_->GetRowBuffer());
            dataSlice->UpperLimit().KeyBound = ShortenKeyBound(dataSlice->UpperLimit().KeyBound, prefixLength, TaskHost_->GetRowBuffer());

            if (UseNewSortedPool_) {
                dataSlice->IsTeleportable = !inputStreamDescriptor.IsVersioned() &&
                    dataSlice->GetSingleUnversionedChunk()->IsLargeCompleteChunk(Controller_->GetMinTeleportChunkSize());
                if (!inputStreamDescriptor.IsVersioned()) {
                    YT_VERIFY(dataSlice->LowerLimit().KeyBound && !dataSlice->LowerLimit().KeyBound.IsUniversal());
                    YT_VERIFY(dataSlice->UpperLimit().KeyBound && !dataSlice->UpperLimit().KeyBound.IsUniversal());
                }
            } else {
                dataSlice->TransformToLegacy(Controller_->GetRowBuffer());
            }
        }

    protected:
        TSortedControllerBase* Controller_;
        TSortedChunkPoolOptions Options_;
        bool UseNewSortedPool_;

        //! Initialized in descendandt tasks.
        IPersistentChunkPoolPtr ChunkPool_;

        i64 TotalOutputRowCount_ = 0;

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
            result.SetJobProxyMemory(Controller_->GetFinalIOMemorySize(Controller_->Spec_->JobIO, statistics));
            AddFootprintAndUserJobResources(result);
            return result;
        }

        EJobType GetJobType() const override
        {
            return Controller_->GetJobType();
        }

        TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller_->GetUserJobSpec();
        }

        void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            VERIFY_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

            jobSpec->CopyFrom(Controller_->JobSpecTemplate_);
            BuildInputOutputJobSpec(joblet, jobSpec);
        }

        TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobCompleted(joblet, jobSummary);
            if (jobSummary.TotalOutputDataStatistics) {
                TotalOutputRowCount_ += jobSummary.TotalOutputDataStatistics->row_count();
            }

            RegisterOutput(jobSummary, joblet->ChunkListIds, joblet);

            return result;
        }

        TJobFinishedResult OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            return TTask::OnJobAborted(joblet, jobSummary);
        }

        void OnChunkTeleported(TInputChunkPtr teleportChunk, std::any tag) override
        {
            TTask::OnChunkTeleported(teleportChunk, tag);

            // If teleport chunks were found, then teleport table index should be non-null.
            Controller_->RegisterTeleportChunk(
                std::move(teleportChunk), /*key=*/0, /*tableIndex=*/*Controller_->GetOutputTeleportTableIndex());
        }

        TJobSplitterConfigPtr GetJobSplitterConfig() const override
        {
            auto config = TaskHost_->GetJobSplitterConfigTemplate();

            config->EnableJobSplitting &=
                (IsJobInterruptible() &&
                std::ssize(Controller_->InputTables_) <= Controller_->Options_->JobSplitter->MaxInputTableCount);

            return config;
        }

        bool IsJobInterruptible() const override
        {
            if (!TTask::IsJobInterruptible()) {
                return false;
            }

            auto totalJobCount = Controller_->GetTotalJobCounter()->GetTotal();
            return
                !(Controller_->AutoMergeTask_ && CanLoseJobs()) &&
                !Controller_->HasStaticJobDistribution() &&
                2 * Controller_->Options_->MaxOutputTablesTimesJobsCount > totalJobCount * std::ssize(Controller_->GetOutputTablePaths()) &&
                2 * Controller_->Options_->MaxJobCount > totalJobCount;
        }
    };

    INHERIT_DYNAMIC_PHOENIX_TYPE(TSortedTaskBase, TSortedTask, 0xbbe534a7);
    INHERIT_DYNAMIC_PHOENIX_TYPE_TEMPLATED(TAutoMergeableOutputMixin, TAutoMergeableSortedTask, 0x1233fa99, TSortedTaskBase);

    using TSortedTaskPtr = TIntrusivePtr<TSortedTaskBase>;

    TSortedTaskPtr SortedTask_;

    //! The (adjusted) sort columns that define the sort order inside sorted chunk pool.
    TSortColumns PrimarySortColumns_;
    TSortColumns ForeignSortColumns_;

    // TODO(max42): YT-14081.
    // New sorted pool performs unwanted cuts even for small tests. In order to overcome that,
    // we consider total data slice weight instead of total chunk data weight.
    i64 TotalPrimaryInputDataSliceWeight_ = 0;
    i64 TotalForeignInputDataSliceWeight_ = 0;

    // XXX(max42): this field is effectively transient, do not persist it.
    IJobSizeConstraintsPtr JobSizeConstraints_;

    i64 InputSliceDataWeight_;

    IFetcherChunkScraperPtr FetcherChunkScraper_;

    // Custom bits of preparation pipeline.

    bool IsCompleted() const override
    {
        return TOperationControllerBase::IsCompleted() && (!SortedTask_ || SortedTask_->IsCompleted());
    }

    i64 GetUnavailableInputChunkCount() const override
    {
        if (FetcherChunkScraper_ && State == EControllerState::Preparing) {
            return FetcherChunkScraper_->GetUnavailableChunkCount();
        }

        return TOperationControllerBase::GetUnavailableInputChunkCount();
    }

    void CalculateSizes()
    {
        Spec_->Sampling->MaxTotalSliceCount = Spec_->Sampling->MaxTotalSliceCount.value_or(Config->MaxTotalSliceCount);

        switch (OperationType) {
            case EOperationType::Merge:
                JobSizeConstraints_ = CreateMergeJobSizeConstraints(
                    Spec_,
                    Options_,
                    Logger,
                    TotalEstimatedInputChunkCount,
                    PrimaryInputDataWeight,
                    DataWeightRatio,
                    InputCompressionRatio,
                    InputTables_.size(),
                    GetPrimaryInputTableCount());
                break;
            default:
                JobSizeConstraints_ = CreateUserJobSizeConstraints(
                    Spec_,
                    Options_,
                    Logger,
                    OutputTables_.size(),
                    DataWeightRatio,
                    TotalEstimatedInputChunkCount,
                    PrimaryInputDataWeight,
                    std::numeric_limits<i64>::max() / 4 /*InputRowCount*/, // It is not important in sorted operations.
                    GetForeignInputDataWeight(),
                    InputTables_.size(),
                    GetPrimaryInputTableCount(),
                    /*sortedOperation=*/true);
                break;
        }

        InputSliceDataWeight_ = JobSizeConstraints_->GetInputSliceDataWeight();

        YT_LOG_INFO(
            "Calculated operation parameters (JobCount: %v, MaxDataWeightPerJob: %v, InputSliceDataWeight: %v)",
            JobSizeConstraints_->GetJobCount(),
            JobSizeConstraints_->GetMaxDataWeightPerJob(),
            InputSliceDataWeight_);
    }

    void CustomPrepare() override
    {
        TOperationControllerBase::CustomPrepare();

        InitTeleportableInputTables();
    }

    void CheckInputTableSortColumnTypes(
        const TSortColumns& sortColumns,
        std::function<bool(const TInputTablePtr& table)> inputTableFilter = [] (const TInputTablePtr& /*table*/) { return true; })
    {
        YT_VERIFY(!InputTables_.empty());

        for (const auto& sortColumn : sortColumns) {
            const TColumnSchema* referenceColumn = nullptr;
            TInputTablePtr referenceTable;
            for (const auto& table : InputTables_) {
                if (!inputTableFilter(table)) {
                    continue;
                }
                const auto& column = table->Schema->GetColumnOrThrow(sortColumn.Name);
                if (column.IsOfV1Type(ESimpleLogicalValueType::Any)) {
                    continue;
                }
                if (referenceColumn) {
                    bool ok = true;
                    if (!referenceColumn->IsOfV1Type() || !column.IsOfV1Type()) {
                        if (*column.LogicalType() != *referenceColumn->LogicalType())  {
                            ok = false;
                        }
                    } else if (GetPhysicalType(referenceColumn->CastToV1Type()) != GetPhysicalType(column.CastToV1Type())) {
                        ok = false;
                    }
                    if (!ok) {
                        THROW_ERROR_EXCEPTION("Sort columns have different types in input tables")
                                << TErrorAttribute("column_name", sortColumn.Name)
                                << TErrorAttribute("input_table_1", referenceTable->GetPath())
                                << TErrorAttribute("type_1", ToString(*referenceColumn->LogicalType()))
                                << TErrorAttribute("input_table_2", table->GetPath())
                                << TErrorAttribute("type_2", ToString(*column.LogicalType()));
                    }
                    if (referenceColumn->SortOrder() != column.SortOrder()) {
                        THROW_ERROR_EXCEPTION("Sort columns have different sort orders in input tables")
                                << TErrorAttribute("column_name", sortColumn.Name)
                                << TErrorAttribute("input_table_1", referenceTable->GetPath())
                                << TErrorAttribute("sort_order_1", *referenceColumn->SortOrder())
                                << TErrorAttribute("input_table_2", table->GetPath())
                                << TErrorAttribute("sort_order_2", *column.SortOrder());
                    }
                } else {
                    referenceColumn = &column;
                    referenceTable = table;
                }
            }
        }
    }

    TChunkStripePtr CreateChunkStripe(TLegacyDataSlicePtr dataSlice)
    {
        auto chunkStripe = New<TChunkStripe>(InputTables_[dataSlice->GetTableIndex()]->IsForeign());
        chunkStripe->DataSlices.emplace_back(std::move(dataSlice));
        return chunkStripe;
    }

    void ProcessInputs()
    {
        YT_PROFILE_TIMING("/operations/merge/input_processing_time") {
            YT_LOG_INFO("Processing inputs");

            TPeriodicYielder yielder(PrepareYieldPeriod);

            SortedTask_->SetIsInput(true);

            int primaryUnversionedSlices = 0;
            int primaryVersionedSlices = 0;
            int foreignSlices = 0;
            // TODO(max42): use CollectPrimaryInputDataSlices() here?
            for (const auto& chunk : CollectPrimaryUnversionedChunks()) {
                const auto& comparator = InputTables_[chunk->GetTableIndex()]->Comparator;
                YT_VERIFY(comparator);

                const auto& dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
                dataSlice->SetInputStreamIndex(InputStreamDirectory_.GetInputStreamIndex(chunk->GetTableIndex(), chunk->GetRangeIndex()));
                if (comparator) {
                    dataSlice->TransformToNew(RowBuffer, comparator.GetLength());
                    InferLimitsFromBoundaryKeys(dataSlice, RowBuffer, comparator);
                } else {
                    dataSlice->TransformToNewKeyless();
                }

                TotalPrimaryInputDataSliceWeight_ += dataSlice->GetDataWeight();

                SortedTask_->AddInput(CreateChunkStripe(dataSlice));
                ++primaryUnversionedSlices;
                yielder.TryYield();
            }
            for (const auto& slice : CollectPrimaryVersionedDataSlices(InputSliceDataWeight_)) {
                // If we keep chunk slice limits at this point, they will be transformed to legacy
                // limits back and forth in legacy sorted chunk pool. Right now it breaks them, e.g.
                // >=[1,1] -> [1,1] -> >[1]. I am not completely sure how to fix that properly, so
                // I am removing chunk slice limits as a workaround until YT-13880.
                for (const auto& chunkSlice : slice->ChunkSlices) {
                    chunkSlice->LowerLimit() = TInputSliceLimit();
                    chunkSlice->UpperLimit() = TInputSliceLimit();
                }

                SortedTask_->AddInput(CreateChunkStripe(slice));

                TotalPrimaryInputDataSliceWeight_ += slice->GetDataWeight();

                ++primaryVersionedSlices;
                yielder.TryYield();
            }
            for (const auto& tableSlices : CollectForeignInputDataSlices(ForeignSortColumns_.size())) {
                for (const auto& slice : tableSlices) {
                    SortedTask_->AddInput(CreateChunkStripe(slice));

                    TotalForeignInputDataSliceWeight_ += slice->GetDataWeight();

                    ++foreignSlices;
                    yielder.TryYield();
                }
            }

            YT_LOG_INFO("Processed inputs (PrimaryUnversionedSlices: %v, PrimaryVersionedSlices: %v, ForeignSlices: %v)",
                primaryUnversionedSlices,
                primaryVersionedSlices,
                foreignSlices);
        }
    }

    void FinishPreparation()
    {
        InitJobIOConfig();
        InitJobSpecTemplate();
    }

    virtual std::optional<int> GetOutputTeleportTableIndex() const = 0;

    //! Initializes #JobIOConfig.
    void InitJobIOConfig()
    {
        JobIOConfig_ = CloneYsonStruct(Spec_->JobIO);
    }

    virtual bool IsKeyGuaranteeEnabled() = 0;

    virtual EJobType GetJobType() const = 0;

    virtual TCpuResource GetCpuLimit() const = 0;

    virtual void InitJobSpecTemplate() = 0;

    void InitTeleportableInputTables()
    {
        auto tableIndex = GetOutputTeleportTableIndex();
        if (tableIndex) {
            for (const auto& inputTable : InputTables_) {
                if (inputTable->SupportsTeleportation() && OutputTables_[*tableIndex]->SupportsTeleportation()) {
                    inputTable->Teleportable = CheckTableSchemaCompatibility(
                        *inputTable->Schema,
                        *OutputTables_[*tableIndex]->TableUploadOptions.TableSchema.Get(),
                        false /*ignoreSortOrder*/).first == ESchemaCompatibility::FullyCompatible;
                    if (GetJobType() == EJobType::SortedReduce) {
                        inputTable->Teleportable &= inputTable->Path.GetTeleport();
                    }
                }
            }
        }
    }

    virtual bool ShouldSlicePrimaryTableByKeys() const
    {
        return true;
    }

    virtual i64 GetMinTeleportChunkSize()  = 0;

    virtual void AdjustSortColumns() = 0;

    virtual i64 GetForeignInputDataWeight() const = 0;

    void PrepareOutputTables() override
    {
        // NB: we need to do this after locking input tables but before preparing output tables.
        AdjustSortColumns();
    }

    virtual TUserJobSpecPtr GetUserJobSpec() const = 0;

    void CustomMaterialize() override
    {
        // NB: Base member is not called intentionally.
        // TODO(max42): But why?

        CalculateSizes();

        bool autoMergeNeeded = false;
        if (GetOperationType() != EOperationType::Merge) {
            autoMergeNeeded = TryInitAutoMerge(JobSizeConstraints_->GetJobCount());
        }

        if (autoMergeNeeded) {
            SortedTask_ = New<TAutoMergeableSortedTask>(this, GetAutoMergeStreamDescriptors(), std::vector<TInputStreamDescriptorPtr>{});
            AutoMergeTask_->SetInputStreamDescriptors(
                BuildInputStreamDescriptorsFromOutputStreamDescriptors(SortedTask_->GetOutputStreamDescriptors()));
        } else {
            SortedTask_ = New<TSortedTask>(this, GetStandardStreamDescriptors(), std::vector<TInputStreamDescriptorPtr>{});
        }
        RegisterTask(SortedTask_);

        ProcessInputs();

        // YT-14081.
        if (ParseOperationSpec<TSortedOperationSpec>(ConvertToNode(Spec_))->UseNewSortedPool) {
            JobSizeConstraints_->UpdateInputDataWeight(TotalForeignInputDataSliceWeight_ + TotalPrimaryInputDataSliceWeight_);
            JobSizeConstraints_->UpdatePrimaryInputDataWeight(TotalPrimaryInputDataSliceWeight_);
        }

        FinishTaskInput(SortedTask_);
        if (AutoMergeTask_) {
            AutoMergeTask_->RegisterInGraph(SortedTask_->GetVertexDescriptor());
        }

        FinishPreparation();
    }

    bool IsBoundaryKeysFetchEnabled() const override
    {
        return true;
    }

    virtual bool HasStaticJobDistribution() const
    {
        return false;
    }

    class TChunkSliceFetcherFactory
        : public IChunkSliceFetcherFactory
    {
    public:
        //! Used only for persistence.
        TChunkSliceFetcherFactory() = default;

        TChunkSliceFetcherFactory(TSortedControllerBase* controller)
            : Controller_(controller)
        { }

        IChunkSliceFetcherPtr CreateChunkSliceFetcher() override
        {
            return Controller_->CreateChunkSliceFetcher();
        }

        void Persist(const TPersistenceContext& context) override
        {
            using NYT::Persist;

            Persist(context, Controller_);
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TChunkSliceFetcherFactory, 0x23cad49e);

        TSortedControllerBase* Controller_;
    };

    virtual IChunkSliceFetcherFactoryPtr CreateChunkSliceFetcherFactory()
    {
        return New<TChunkSliceFetcherFactory>(this /*controller*/);
    }

    virtual TSortedChunkPoolOptions GetSortedChunkPoolOptions()
    {
        TSortedChunkPoolOptions chunkPoolOptions;
        TSortedJobOptions jobOptions;
        jobOptions.EnableKeyGuarantee = IsKeyGuaranteeEnabled();
        jobOptions.PrimaryComparator = GetComparator(PrimarySortColumns_);
        jobOptions.ForeignComparator = GetComparator(ForeignSortColumns_);
        jobOptions.PrimaryPrefixLength = PrimarySortColumns_.size();
        jobOptions.ForeignPrefixLength = ForeignSortColumns_.size();
        jobOptions.ShouldSlicePrimaryTableByKeys = ShouldSlicePrimaryTableByKeys();
        jobOptions.MaxTotalSliceCount = Config->MaxTotalSliceCount;
        jobOptions.EnablePeriodicYielder = true;

        chunkPoolOptions.RowBuffer = RowBuffer;
        chunkPoolOptions.SortedJobOptions = jobOptions;
        chunkPoolOptions.MinTeleportChunkSize = GetMinTeleportChunkSize();
        chunkPoolOptions.JobSizeConstraints = JobSizeConstraints_;
        chunkPoolOptions.Logger = Logger.WithTag("Name: Root");
        chunkPoolOptions.StructuredLogger = ChunkPoolStructuredLogger
            .WithStructuredTag("operation_id", OperationId);
        return chunkPoolOptions;
    }

    void OnChunksReleased(int chunkCount) override
    {
        TOperationControllerBase::OnChunksReleased(chunkCount);

        if (const auto& autoMergeDirector = GetAutoMergeDirector()) {
            autoMergeDirector->OnMergeJobFinished(chunkCount /*unregisteredIntermediateChunkCount*/);
        }
    }

    EIntermediateChunkUnstageMode GetIntermediateChunkUnstageMode() const override
    {
        auto reducerSpec = GetUserJobSpec();
        // We could get here only if this is a sorted reduce and auto-merge is enabled.
        YT_VERIFY(reducerSpec);
        YT_VERIFY(Spec_->AutoMerge->Mode != EAutoMergeMode::Disabled);

        if (Spec_->AutoMerge->Mode != EAutoMergeMode::Relaxed && reducerSpec->Deterministic) {
            return EIntermediateChunkUnstageMode::OnJobCompleted;
        } else {
            return EIntermediateChunkUnstageMode::OnSnapshotCompleted;
        }
    }

private:
    IChunkSliceFetcherPtr CreateChunkSliceFetcher()
    {
        FetcherChunkScraper_ = CreateFetcherChunkScraper();

        auto fetcher = NTableClient::CreateChunkSliceFetcher(
            Config->ChunkSliceFetcher,
            InputNodeDirectory_,
            GetCancelableInvoker(),
            FetcherChunkScraper_,
            Host->GetClient(),
            RowBuffer,
            Logger);
        fetcher->SetCancelableContext(GetCancelableContext());
        return fetcher;
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TSortedControllerBase::TSortedTask);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortedControllerBase::TAutoMergeableSortedTask);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortedControllerBase::TChunkSliceFetcherFactory);

////////////////////////////////////////////////////////////////////////////////

class TSortedMergeController
    : public TSortedControllerBase
{
public:
    TSortedMergeController(
        TSortedMergeOperationSpecPtr spec,
        TControllerAgentConfigPtr config,
        TSortedMergeOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation)
        : TSortedControllerBase(
            spec,
            config,
            options,
            host,
            operation)
        , Spec_(spec)
    { }

    bool ShouldSlicePrimaryTableByKeys() const override
    {
        return true;
    }

    bool IsRowCountPreserved() const override
    {
        return !Spec_->Sampling->SamplingRate &&
            !Spec_->JobIO->TableReader->SamplingRate;
    }

    TUserJobSpecPtr GetUserJobSpec() const override
    {
        return nullptr;
    }

    i64 GetMinTeleportChunkSize() override
    {
        if (Spec_->ForceTransform) {
            return std::numeric_limits<i64>::max() / 4;
        }
        if (!Spec_->CombineChunks) {
            return 0;
        }
        return Spec_->JobIO
            ->TableWriter
            ->DesiredChunkSize;
    }

    void AdjustSortColumns() override
    {
        const auto& specSortColumns = Spec_->MergeBy;
        YT_LOG_INFO("Spec sort columns obtained (SortColumns: %v)",
            specSortColumns);

        PrimarySortColumns_ = CheckInputTablesSorted(specSortColumns);
        YT_LOG_INFO("Sort columns adjusted (SortColumns: %v)",
            PrimarySortColumns_);
    }

    bool IsKeyGuaranteeEnabled() override
    {
        return false;
    }

    EJobType GetJobType() const override
    {
        return EJobType::SortedMerge;
    }

    TCpuResource GetCpuLimit() const override
    {
        return 1;
    }

    std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec_->InputTablePaths;
    }

    std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return {Spec_->OutputTablePath};
    }

    void InitJobSpecTemplate() override
    {
        JobSpecTemplate_.set_type(static_cast<int>(EJobType::SortedMerge));
        auto* jobSpecExt = JobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);
        auto* mergeJobSpecExt = JobSpecTemplate_.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);
        jobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec_->JobIO)).ToString());

        SetProtoExtension<NChunkClient::NProto::TDataSourceDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            BuildDataSourceDirectoryFromInputTables(InputTables_));
        SetProtoExtension<NChunkClient::NProto::TDataSinkDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            BuildDataSinkDirectoryFromOutputTables(OutputTables_));
        jobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig_).ToString());

        ToProto(mergeJobSpecExt->mutable_key_columns(), GetColumnNames(PrimarySortColumns_));
        ToProto(mergeJobSpecExt->mutable_sort_columns(), PrimarySortColumns_);
    }

    std::optional<int> GetOutputTeleportTableIndex() const override
    {
        return std::make_optional(0);
    }

    void PrepareOutputTables() override
    {
        // Check that all input tables are sorted by the same key columns.
        TSortedControllerBase::PrepareOutputTables();

        auto& table = OutputTables_[0];
        if (!table->Dynamic) {
            table->TableUploadOptions.LockMode = ELockMode::Exclusive;
        }

        ValidateSchemaInferenceMode(Spec_->SchemaInferenceMode);

        auto prepareOutputSortColumns = [&] {
            if (table->TableUploadOptions.TableSchema->IsSorted()) {
                if (table->TableUploadOptions.TableSchema->GetSortColumns() != PrimarySortColumns_) {
                    THROW_ERROR_EXCEPTION("Merge sort columns do not match output table schema in \"strong\" schema mode")
                        << TErrorAttribute("output_schema", *table->TableUploadOptions.TableSchema)
                        << TErrorAttribute("merge_by", PrimarySortColumns_)
                        << TErrorAttribute("schema_inference_mode", Spec_->SchemaInferenceMode);
                }
            } else {
                table->TableUploadOptions.TableSchema =
                    table->TableUploadOptions.TableSchema->ToSorted(PrimarySortColumns_);
            }
        };

        switch (Spec_->SchemaInferenceMode) {
            case ESchemaInferenceMode::Auto:
                if (table->TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    InferSchemaFromInput(PrimarySortColumns_);
                } else {
                    prepareOutputSortColumns();
                    ValidateOutputSchemaCompatibility(true);
                }
                break;

            case ESchemaInferenceMode::FromInput:
                InferSchemaFromInput(PrimarySortColumns_);
                break;

            case ESchemaInferenceMode::FromOutput:
                if (table->TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    table->TableUploadOptions.TableSchema = TTableSchema::FromSortColumns(PrimarySortColumns_);
                } else {
                    prepareOutputSortColumns();
                }
                break;

            default:
                YT_ABORT();
        }
    }

    i64 GetForeignInputDataWeight() const override
    {
        return 0;
    }

protected:
    TStringBuf GetDataWeightParameterNameForJob(EJobType /*jobType*/) const override
    {
        return TStringBuf("data_weight_per_job");
    }

    std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {EJobType::SortedMerge};
    }

    TYsonStructPtr GetTypedSpec() const override
    {
        return Spec_;
    }

    void OnOperationCompleted(bool interrupted) override
    {
        if (!interrupted) {
            auto isNontrivialInput = InputHasReadLimits() || InputHasVersionedTables();
            if (!isNontrivialInput && IsRowCountPreserved() && Spec_->ForceTransform) {
                YT_LOG_ERROR_IF(TotalEstimatedInputRowCount != SortedTask_->GetTotalOutputRowCount(),
                    "Input/output row count mismatch in sorted merge operation (TotalEstimatedInputRowCount: %v, TotalOutputRowCount: %v)",
                    TotalEstimatedInputRowCount,
                    SortedTask_->GetTotalOutputRowCount());
                YT_VERIFY(TotalEstimatedInputRowCount == SortedTask_->GetTotalOutputRowCount());
            }
        }

        TSortedControllerBase::OnOperationCompleted(interrupted);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedMergeController, 0xf3b791ca);

    TSortedMergeOperationSpecPtr Spec_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TSortedMergeController);

IOperationControllerPtr CreateSortedMergeController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto options = config->SortedMergeOperationOptions;
    auto spec = ParseOperationSpec<TSortedMergeOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
    return New<TSortedMergeController>(spec, config, options, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

class TReduceController
    : public TSortedControllerBase
{
public:
    TReduceController(
        TReduceOperationSpecPtr spec,
        TControllerAgentConfigPtr config,
        TReduceOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation)
        : TSortedControllerBase(
            spec,
            std::move(config),
            options,
            std::move(host),
            operation)
        , Spec_(std::move(spec))
        , Options_(std::move(options))
    { }

    bool IsRowCountPreserved() const override
    {
        return false;
    }

    bool AreForeignTablesSupported() const override
    {
        return true;
    }

    TCpuResource GetCpuLimit() const override
    {
        return TCpuResource(Spec_->Reducer->CpuLimit);
    }

    TUserJobSpecPtr GetUserJobSpec() const override
    {
        return Spec_->Reducer;
    }

    std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec_->InputTablePaths;
    }

    std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return Spec_->OutputTablePaths;
    }

    std::optional<int> GetOutputTeleportTableIndex() const override
    {
        return OutputTeleportTableIndex_;
    }

    i64 GetMinTeleportChunkSize() override
    {
        return 0;
    }

    void CustomizeJoblet(const TJobletPtr& joblet) override
    {
        joblet->StartRowIndex = StartRowIndex_;
        StartRowIndex_ += joblet->InputStripeList->TotalRowCount;
    }

    std::vector<TUserJobSpecPtr> GetUserJobSpecs() const override
    {
        return {Spec_->Reducer};
    }

    void InitJobSpecTemplate() override
    {
        YT_VERIFY(!PrimarySortColumns_.empty());

        JobSpecTemplate_.set_type(static_cast<int>(GetJobType()));
        auto* jobSpecExt = JobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);
        jobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec_->JobIO)).ToString());

        SetProtoExtension<NChunkClient::NProto::TDataSourceDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            BuildDataSourceDirectoryFromInputTables(InputTables_));
        SetProtoExtension<NChunkClient::NProto::TDataSinkDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            BuildDataSinkDirectoryWithAutoMerge(
                OutputTables_,
                AutoMergeEnabled_,
                GetSpec()->AutoMerge->UseIntermediateDataAccount
                    ? std::make_optional(GetSpec()->IntermediateDataAccount)
                    : std::nullopt));

        jobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig_).ToString());

        InitUserJobSpecTemplate(
            jobSpecExt->mutable_user_job_spec(),
            Spec_->Reducer,
            UserJobFiles_[Spec_->Reducer],
            Spec_->DebugArtifactsAccount);

        auto* reduceJobSpecExt = JobSpecTemplate_.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        ToProto(reduceJobSpecExt->mutable_key_columns(), GetColumnNames(SortColumns_));
        ToProto(reduceJobSpecExt->mutable_sort_columns(), SortColumns_);
        reduceJobSpecExt->set_reduce_key_column_count(PrimarySortColumns_.size());
        reduceJobSpecExt->set_join_key_column_count(ForeignSortColumns_.size());

        if (Spec_->ForeignTableLookupKeysThreshold) {
            reduceJobSpecExt->set_foreign_table_lookup_keys_threshold(
                *Spec_->ForeignTableLookupKeysThreshold);
        }
    }

    void CustomPrepare() override
    {
        int teleportOutputCount = 0;
        for (int index = 0; index < static_cast<int>(OutputTables_.size()); ++index) {
            if (OutputTables_[index]->Path.GetTeleport()) {
                ++teleportOutputCount;
                OutputTeleportTableIndex_ = index;
            }
        }

        if (teleportOutputCount > 1) {
            THROW_ERROR_EXCEPTION("Too many teleport output tables: maximum allowed 1, actual %v",
                teleportOutputCount);
        }

        ValidateUserFileCount(Spec_->Reducer, "reducer");

        int foreignInputCount = 0;
        for (auto& table : InputTables_) {
            if (table->Path.GetForeign()) {
                if (table->Path.GetTeleport()) {
                    THROW_ERROR_EXCEPTION("Foreign table can not be specified as teleport")
                        << TErrorAttribute("path", table->Path);
                }
                if (table->Path.GetNewRanges(table->Comparator).size() > 1) {
                    THROW_ERROR_EXCEPTION("Reduce operation does not support foreign tables with multiple ranges");
                }
                ++foreignInputCount;
            }
        }

        if (foreignInputCount == std::ssize(InputTables_)) {
            THROW_ERROR_EXCEPTION("At least one non-foreign input table is required");
        }

        if (foreignInputCount == 0 && !Spec_->JoinBy.empty()) {
            THROW_ERROR_EXCEPTION("At least one foreign input table is required when join_by is specified");
        }

        if (foreignInputCount != 0 && Spec_->JoinBy.empty()) {
            THROW_ERROR_EXCEPTION("It is required to specify join_by when using foreign tables");
        }

        // NB: base class call method is called at the end since InitTeleportableInputTables()
        // relies on OutputTeleportTableIndex_ being set.
        TSortedControllerBase::CustomPrepare();
    }

    void BuildBriefSpec(TFluentMap fluent) const override
    {
        TSortedControllerBase::BuildBriefSpec(fluent);
        fluent
            .Item("reducer").BeginMap()
                .Item("command").Value(TrimCommandForBriefSpec(Spec_->Reducer->Command))
            .EndMap();
    }

    bool IsInputDataSizeHistogramSupported() const override
    {
        return true;
    }

    std::optional<TRichYPath> GetStderrTablePath() const override
    {
        return Spec_->StderrTablePath;
    }

    TBlobTableWriterConfigPtr GetStderrTableWriterConfig() const override
    {
        return Spec_->StderrTableWriter;
    }

    std::optional<TRichYPath> GetCoreTablePath() const override
    {
        return Spec_->CoreTablePath;
    }

    TBlobTableWriterConfigPtr GetCoreTableWriterConfig() const override
    {
        return Spec_->CoreTableWriter;
    }

    bool GetEnableCudaGpuCoreDump() const override
    {
        return Spec_->EnableCudaGpuCoreDump;
    }

    ELegacyLivePreviewMode GetLegacyOutputLivePreviewMode() const override
    {
        return ToLegacyLivePreviewMode(Spec_->EnableLegacyLivePreview);
    }

    i64 GetForeignInputDataWeight() const override
    {
        return Spec_->ConsiderOnlyPrimarySize ? 0 : ForeignInputDataWeight;
    }

    TYsonStructPtr GetTypedSpec() const override
    {
        return Spec_;
    }

    bool ShouldSlicePrimaryTableByKeys() const override
    {
        return *Spec_->EnableKeyGuarantee;
    }

    EJobType GetJobType() const override
    {
        return *Spec_->EnableKeyGuarantee ? EJobType::SortedReduce : EJobType::JoinReduce;
    }

    bool IsKeyGuaranteeEnabled() override
    {
        return *Spec_->EnableKeyGuarantee;
    }

    void AdjustSortColumns() override
    {
        YT_LOG_INFO("Adjusting sort columns (EnableKeyGuarantee: %v, ReduceBy: %v, SortBy: %v, JoinBy: %v)",
            *Spec_->EnableKeyGuarantee,
            Spec_->ReduceBy,
            Spec_->SortBy,
            Spec_->JoinBy);

        if (*Spec_->EnableKeyGuarantee) {
            auto specKeyColumns = Spec_->SortBy.empty() ? Spec_->ReduceBy : Spec_->SortBy;
            SortColumns_ = CheckInputTablesSorted(specKeyColumns, &TInputTable::IsPrimary);

            if (!CheckSortColumnsCompatible(SortColumns_, Spec_->ReduceBy)) {
                THROW_ERROR_EXCEPTION("Reduce sort columns are not compatible with sort columns")
                    << TErrorAttribute("reduce_by", Spec_->ReduceBy)
                    << TErrorAttribute("sort_by", SortColumns_);
            }

            if (Spec_->ReduceBy.empty()) {
                THROW_ERROR_EXCEPTION("Reduce by can not be empty when key guarantee is enabled")
                    << TErrorAttribute("operation_type", OperationType);
            }

            PrimarySortColumns_ = Spec_->ReduceBy;
            ForeignSortColumns_ = Spec_->JoinBy;
            if (!ForeignSortColumns_.empty()) {
                CheckInputTablesSorted(ForeignSortColumns_, &TInputTable::IsForeign);
                if (!CheckSortColumnsCompatible(PrimarySortColumns_, ForeignSortColumns_)) {
                    THROW_ERROR_EXCEPTION("Join sort columns are not compatible with reduce sort columns")
                        << TErrorAttribute("join_by", ForeignSortColumns_)
                        << TErrorAttribute("reduce_by", PrimarySortColumns_);
                }
            }
        } else {
            if (!Spec_->ReduceBy.empty() && !Spec_->JoinBy.empty()) {
                THROW_ERROR_EXCEPTION("Specifying both reduce and join key columns is not supported in disabled key guarantee mode");
            }
            if (Spec_->ReduceBy.empty() && Spec_->JoinBy.empty()) {
                THROW_ERROR_EXCEPTION("At least one of reduce_by or join_by is required for this operation");
            }
            PrimarySortColumns_ = CheckInputTablesSorted(!Spec_->ReduceBy.empty() ? Spec_->ReduceBy : Spec_->JoinBy);
            if (PrimarySortColumns_.empty()) {
                THROW_ERROR_EXCEPTION("At least one of reduce_by and join_by should be specified when key guarantee is disabled")
                    << TErrorAttribute("operation_type", OperationType);
            }
            SortColumns_ = ForeignSortColumns_ = PrimarySortColumns_;

            if (!Spec_->SortBy.empty()) {
                if (!CheckSortColumnsCompatible(Spec_->SortBy, Spec_->JoinBy)) {
                    THROW_ERROR_EXCEPTION("Join sort columns are not compatible with sort columns")
                        << TErrorAttribute("join_by", Spec_->JoinBy)
                        << TErrorAttribute("sort_by", Spec_->SortBy);
                }
                SortColumns_ = Spec_->SortBy;
            }
        }
        if (Spec_->ValidateKeyColumnTypes) {
            CheckInputTableSortColumnTypes(ForeignSortColumns_);
            CheckInputTableSortColumnTypes(PrimarySortColumns_, [] (const auto& table) {
                return table->IsPrimary();
            });
        }
        YT_LOG_INFO("Sort columns adjusted (PrimarySortColumns: %v, ForeignSortColumns: %v, SortColumns: %v)",
            PrimarySortColumns_,
            ForeignSortColumns_,
            SortColumns_);

        if (!Spec_->PivotKeys.empty()) {
            if (!*Spec_->EnableKeyGuarantee) {
                THROW_ERROR_EXCEPTION("Pivot keys are not supported in disabled key guarantee mode");
            }

            auto comparator = GetComparator(SortColumns_);
            TKeyBound previousUpperBound;
            for (const auto& key : Spec_->PivotKeys) {
                if (key.GetCount() > std::ssize(Spec_->ReduceBy)) {
                    THROW_ERROR_EXCEPTION("Pivot key cannot be longer than reduce key column count")
                        << TErrorAttribute("key", key)
                        << TErrorAttribute("reduce_by", Spec_->ReduceBy);
                }

                auto upperBound = TKeyBound::FromRow() < key;
                if (previousUpperBound && comparator.CompareKeyBounds(upperBound, previousUpperBound) <= 0) {
                    THROW_ERROR_EXCEPTION("Pivot keys should form a strictly increasing sequence")
                        << TErrorAttribute("previous", previousUpperBound)
                        << TErrorAttribute("current", upperBound)
                        << TErrorAttribute("comparator", comparator);
                }
                previousUpperBound = upperBound;
            }
            for (const auto& table : InputTables_) {
                if (table->Path.GetTeleport()) {
                    THROW_ERROR_EXCEPTION("Chunk teleportation is not supported when pivot keys are specified");
                }
            }
        }
    }

    bool HasStaticJobDistribution() const override
    {
        return
            TSortedControllerBase::HasStaticJobDistribution() ||
            !Spec_->PivotKeys.empty();
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TReduceController, 0x4fc44a45);

    TReduceOperationSpecPtr Spec_;
    TReduceOperationOptionsPtr Options_;

    i64 StartRowIndex_ = 0;

    TSortColumns SortColumns_;

    std::optional<int> OutputTeleportTableIndex_;

    TStringBuf GetDataWeightParameterNameForJob(EJobType /*jobType*/) const override
    {
        return TStringBuf("data_weight_per_job");
    }

    std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {GetJobType()};
    }

    IChunkSliceFetcherFactoryPtr CreateChunkSliceFetcherFactory() override
    {
        if (Spec_->PivotKeys.empty()) {
            return TSortedControllerBase::CreateChunkSliceFetcherFactory();
        } else {
            return nullptr;
        }
    }

    TSortedChunkPoolOptions GetSortedChunkPoolOptions() override
    {
        auto options = TSortedControllerBase::GetSortedChunkPoolOptions();
        options.SortedJobOptions.PivotKeys = std::vector<TLegacyKey>(Spec_->PivotKeys.begin(), Spec_->PivotKeys.end());
        options.SliceForeignChunks = Spec_->SliceForeignChunks;
        options.SortedJobOptions.ConsiderOnlyPrimarySize = Spec_->ConsiderOnlyPrimarySize;
        return options;
    }

    TError GetAutoMergeError() const override
    {
        return TError();
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TReduceController);

IOperationControllerPtr CreateReduceController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation,
    bool isJoinReduce)
{
    auto options = isJoinReduce ? config->JoinReduceOperationOptions : config->ReduceOperationOptions;
    auto mergedSpec = UpdateSpec(options->SpecTemplate, operation->GetSpec());
    auto spec = ParseOperationSpec<TReduceOperationSpec>(mergedSpec);
    if (!spec->EnableKeyGuarantee) {
        spec->EnableKeyGuarantee = !isJoinReduce;
    }
    return New<TReduceController>(std::move(spec), std::move(config), std::move(options), std::move(host), operation);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
