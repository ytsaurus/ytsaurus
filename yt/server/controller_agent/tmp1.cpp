










































TOperationControllerBase::TTask::TTask()
    : CachedPendingJobCount(-1)
    , CachedTotalJobCount(-1)
    , DemandSanityCheckDeadline(0)
    , CompletedFired(false)
    , Logger(OperationLogger)
{ }

TOperationControllerBase::TTask::TTask(TOperationControllerBase* controller)
    : Controller(controller)
    , CachedPendingJobCount(0)
    , CachedTotalJobCount(0)
    , DemandSanityCheckDeadline(0)
    , CompletedFired(false)
    , Logger(OperationLogger)
{ }

void TOperationControllerBase::TTask::Initialize()
{
    Logger = Controller->Logger;
    Logger.AddTag("Task: %v", GetId());
}

int TOperationControllerBase::TTask::GetPendingJobCount() const
{
    return GetChunkPoolOutput()->GetPendingJobCount();
}

int TOperationControllerBase::TTask::GetPendingJobCountDelta()
{
    int oldValue = CachedPendingJobCount;
    int newValue = GetPendingJobCount();
    CachedPendingJobCount = newValue;
    return newValue - oldValue;
}

int TOperationControllerBase::TTask::GetTotalJobCount() const
{
    return GetChunkPoolOutput()->GetTotalJobCount();
}

int TOperationControllerBase::TTask::GetTotalJobCountDelta()
{
    int oldValue = CachedTotalJobCount;
    int newValue = GetTotalJobCount();
    CachedTotalJobCount = newValue;
    return newValue - oldValue;
}

TNullable<i64> TOperationControllerBase::TTask::GetMaximumUsedTmpfsSize() const
{
    return MaximumUsedTmfpsSize;
}

const TProgressCounter& TOperationControllerBase::TTask::GetJobCounter() const
{
    return GetChunkPoolOutput()->GetJobCounter();
}

TJobResources TOperationControllerBase::TTask::GetTotalNeededResourcesDelta()
{
    auto oldValue = CachedTotalNeededResources;
    auto newValue = GetTotalNeededResources();
    CachedTotalNeededResources = newValue;
    newValue -= oldValue;
    return newValue;
}

TJobResources TOperationControllerBase::TTask::GetTotalNeededResources() const
{
    i64 count = GetPendingJobCount();
    // NB: Don't call GetMinNeededResources if there are no pending jobs.
    return count == 0 ? ZeroJobResources() : GetMinNeededResources() * count;
}

bool TOperationControllerBase::TTask::IsIntermediateOutput() const
{
    return false;
}

bool TOperationControllerBase::TTask::IsStderrTableEnabled() const
{
    // We write stderr if corresponding options were specified and only for user-type jobs.
    // For example we don't write stderr for sort stage in mapreduce operation
    // even if stderr table were specified.
    return Controller->GetStderrTablePath() && GetUserJobSpec();
}

bool TOperationControllerBase::TTask::IsCoreTableEnabled() const
{
    // Same as above.
    return Controller->GetCoreTablePath() && GetUserJobSpec();
}

i64 TOperationControllerBase::TTask::GetLocality(TNodeId nodeId) const
{
    return HasInputLocality()
        ? GetChunkPoolOutput()->GetLocality(nodeId)
        : 0;
}

bool TOperationControllerBase::TTask::HasInputLocality() const
{
    return true;
}

void TOperationControllerBase::TTask::AddInput(TChunkStripePtr stripe)
{
    Controller->RegisterInputStripe(stripe, this);
    if (HasInputLocality()) {
        Controller->AddTaskLocalityHint(this, stripe);
    }
    AddPendingHint();
}

void TOperationControllerBase::TTask::AddInput(const std::vector<TChunkStripePtr>& stripes)
{
    for (auto stripe : stripes) {
        if (stripe) {
            AddInput(stripe);
        }
    }
}

void TOperationControllerBase::TTask::FinishInput()
{
    LOG_DEBUG("Task input finished");

    GetChunkPoolInput()->Finish();
    AddPendingHint();
    CheckCompleted();
}

void TOperationControllerBase::TTask::CheckCompleted()
{
    if (!CompletedFired && IsCompleted()) {
        CompletedFired = true;
        OnTaskCompleted();
    }
}

TUserJobSpecPtr TOperationControllerBase::TTask::GetUserJobSpec() const
{
    return nullptr;
}

void TOperationControllerBase::TTask::ScheduleJob(
    ISchedulingContext* context,
    const TJobResources& jobLimits,
    TScheduleJobResult* scheduleJobResult)
{
    if (!CanScheduleJob(context, jobLimits)) {
        scheduleJobResult->RecordFail(EScheduleJobFailReason::TaskRefusal);
        return;
    }

    bool intermediateOutput = IsIntermediateOutput();
    int jobIndex = Controller->JobIndexGenerator.Next();
    auto joblet = New<TJoblet>(Controller->CreateJobMetricsUpdater(), this, jobIndex);

    const auto& nodeResourceLimits = context->ResourceLimits();
    auto nodeId = context->GetNodeDescriptor().Id;
    const auto& address = context->GetNodeDescriptor().Address;

    auto* chunkPoolOutput = GetChunkPoolOutput();
    auto localityNodeId = HasInputLocality() ? nodeId : InvalidNodeId;
    joblet->OutputCookie = chunkPoolOutput->Extract(localityNodeId);
    if (joblet->OutputCookie == IChunkPoolOutput::NullCookie) {
        LOG_DEBUG("Job input is empty");
        scheduleJobResult->RecordFail(EScheduleJobFailReason::EmptyInput);
        return;
    }

    int sliceCount = chunkPoolOutput->GetStripeListSliceCount(joblet->OutputCookie);
    const auto& jobSpecSliceThrottler = context->GetJobSpecSliceThrottler();
    if (sliceCount > Controller->Config->HeavyJobSpecSliceCountThreshold) {
        if (!jobSpecSliceThrottler->TryAcquire(sliceCount)) {
            LOG_DEBUG("Job spec throttling is active (SliceCount: %v)",
                sliceCount);
            chunkPoolOutput->Aborted(joblet->OutputCookie, EAbortReason::SchedulingJobSpecThrottling);
            scheduleJobResult->RecordFail(EScheduleJobFailReason::JobSpecThrottling);
            return;
        }
    } else {
        jobSpecSliceThrottler->Acquire(sliceCount);
    }

    joblet->InputStripeList = chunkPoolOutput->GetStripeList(joblet->OutputCookie);
    auto estimatedResourceUsage = GetNeededResources(joblet);
    auto neededResources = ApplyMemoryReserve(estimatedResourceUsage);

    joblet->EstimatedResourceUsage = estimatedResourceUsage;
    joblet->ResourceLimits = neededResources;

    // Check the usage against the limits. This is the last chance to give up.
    if (!Dominates(jobLimits, neededResources)) {
        LOG_DEBUG("Job actual resource demand is not met (Limits: %v, Demand: %v)",
            FormatResources(jobLimits),
            FormatResources(neededResources));
        CheckResourceDemandSanity(nodeResourceLimits, neededResources);
        chunkPoolOutput->Aborted(joblet->OutputCookie, EAbortReason::SchedulingOther);
        // Seems like cached min needed resources are too optimistic.
        ResetCachedMinNeededResources();
        scheduleJobResult->RecordFail(EScheduleJobFailReason::NotEnoughResources);
        return;
    }

    // Async part.
    auto controller = MakeStrong(Controller); // hold the controller
    auto jobSpecBuilder = BIND([=, this_ = MakeStrong(this)] (TJobSpec* jobSpec) {
        BuildJobSpec(joblet, jobSpec);
        jobSpec->set_version(GetJobSpecVersion());
        controller->CustomizeJobSpec(joblet, jobSpec);

        auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        if (controller->Spec->JobProxyMemoryOvercommitLimit) {
            schedulerJobSpecExt->set_job_proxy_memory_overcommit_limit(*controller->Spec->JobProxyMemoryOvercommitLimit);
        }
        schedulerJobSpecExt->set_job_proxy_ref_counted_tracker_log_period(ToProto(controller->Spec->JobProxyRefCountedTrackerLogPeriod));
        schedulerJobSpecExt->set_abort_job_if_account_limit_exceeded(controller->Spec->SuspendOperationIfAccountLimitExceeded);

        // Adjust sizes if approximation flag is set.
        if (joblet->InputStripeList->IsApproximate) {
            schedulerJobSpecExt->set_input_data_weight(static_cast<i64>(
                schedulerJobSpecExt->input_data_weight() *
                ApproximateSizesBoostFactor));
            schedulerJobSpecExt->set_input_row_count(static_cast<i64>(
                schedulerJobSpecExt->input_row_count() *
                ApproximateSizesBoostFactor));
        }

        if (schedulerJobSpecExt->input_data_weight() > controller->Spec->MaxDataWeightPerJob) {
            controller->OnOperationFailed(TError(
                "Maximum allowed data weight per job violated: %v > %v",
                schedulerJobSpecExt->input_data_weight(),
                controller->Spec->MaxDataWeightPerJob));
        }
    });

    auto jobType = GetJobType();
    joblet->JobId = context->GenerateJobId();
    auto restarted = LostJobCookieMap.find(joblet->OutputCookie) != LostJobCookieMap.end();
    joblet->Account = Controller->Spec->JobNodeAccount;
    scheduleJobResult->JobStartRequest.Emplace(
        joblet->JobId,
        jobType,
        neededResources,
        Controller->IsJobInterruptible(),
        jobSpecBuilder);

    joblet->Restarted = restarted;
    joblet->JobType = jobType;
    joblet->NodeDescriptor = context->GetNodeDescriptor();
    joblet->JobProxyMemoryReserveFactor = Controller->GetJobProxyMemoryDigest(jobType)->GetQuantile(Controller->Config->JobProxyMemoryReserveQuantile);
    auto userJobSpec = GetUserJobSpec();
    if (userJobSpec) {
        joblet->UserJobMemoryReserveFactor = Controller->GetUserJobMemoryDigest(GetJobType())->GetQuantile(Controller->Config->UserJobMemoryReserveQuantile);
    }

    LOG_DEBUG(
        "Job scheduled (JobId: %v, OperationId: %v, JobType: %v, Address: %v, JobIndex: %v, OutputCookie: %v, SliceCount: %v (%v local), "
        "Approximate: %v, DataWeight: %v (%v local), RowCount: %v, Restarted: %v, EstimatedResourceUsage: %v, JobProxyMemoryReserveFactor: %v, "
        "UserJobMemoryReserveFactor: %v, ResourceLimits: %v)",
        joblet->JobId,
        Controller->OperationId,
        jobType,
        address,
        jobIndex,
        joblet->OutputCookie,
        joblet->InputStripeList->TotalChunkCount,
        joblet->InputStripeList->LocalChunkCount,
        joblet->InputStripeList->IsApproximate,
        joblet->InputStripeList->TotalDataWeight,
        joblet->InputStripeList->LocalDataWeight,
        joblet->InputStripeList->TotalRowCount,
        restarted,
        FormatResources(estimatedResourceUsage),
        joblet->JobProxyMemoryReserveFactor,
        joblet->UserJobMemoryReserveFactor,
        FormatResources(neededResources));

    // Prepare chunk lists.
    if (intermediateOutput) {
        joblet->ChunkListIds.push_back(Controller->ExtractChunkList(Controller->IntermediateOutputCellTag));
    } else {
        for (const auto& table : Controller->OutputTables) {
            joblet->ChunkListIds.push_back(Controller->ExtractChunkList(table.CellTag));
        }
    }

    if (Controller->StderrTable && IsStderrTableEnabled()) {
        joblet->StderrTableChunkListId = Controller->ExtractChunkList(Controller->StderrTable->CellTag);
    }

    if (Controller->CoreTable && IsCoreTableEnabled()) {
        joblet->CoreTableChunkListId = Controller->ExtractChunkList(Controller->CoreTable->CellTag);
    }

    // Sync part.
    PrepareJoblet(joblet);
    Controller->CustomizeJoblet(joblet);

    Controller->RegisterJoblet(joblet);
    Controller->AddValueToEstimatedHistogram(joblet);

    OnJobStarted(joblet);

    if (Controller->JobSplitter_) {
        Controller->JobSplitter_->OnJobStarted(joblet->JobId, joblet->InputStripeList);
    }
}

bool TOperationControllerBase::TTask::IsPending() const
{
    return GetChunkPoolOutput()->GetPendingJobCount() > 0;
}

bool TOperationControllerBase::TTask::IsCompleted() const
{
    return IsActive() && GetChunkPoolOutput()->IsCompleted();
}

bool TOperationControllerBase::TTask::IsActive() const
{
    return true;
}

i64 TOperationControllerBase::TTask::GetTotalDataWeight() const
{
    return GetChunkPoolOutput()->GetTotalDataWeight();
}

i64 TOperationControllerBase::TTask::GetCompletedDataWeight() const
{
    return GetChunkPoolOutput()->GetCompletedDataWeight();
}

i64 TOperationControllerBase::TTask::GetPendingDataWeight() const
{
    return GetChunkPoolOutput()->GetPendingDataWeight();
}

void TOperationControllerBase::TTask::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    // COMPAT(babenko)
    if (context.IsLoad() && context.GetVersion() < 200009) {
        Load<TNullable<TInstant>>(context.LoadContext());
    }

    Persist(context, Controller);

    Persist(context, CachedPendingJobCount);
    Persist(context, CachedTotalJobCount);

    Persist(context, CachedTotalNeededResources);
    Persist(context, CachedMinNeededResources);

    Persist(context, CompletedFired);

    Persist(context, LostJobCookieMap);
}

void TOperationControllerBase::TTask::PrepareJoblet(TJobletPtr /* joblet */)
{ }

void TOperationControllerBase::TTask::OnJobStarted(TJobletPtr joblet)
{ }

void TOperationControllerBase::TTask::OnJobCompleted(TJobletPtr joblet, const TCompletedJobSummary& jobSummary)
{
    YCHECK(jobSummary.Statistics);
    const auto& statistics = *jobSummary.Statistics;

    if (!jobSummary.Abandoned) {
        auto outputStatisticsMap = GetOutputDataStatistics(statistics);
        for (int index = 0; index < static_cast<int>(joblet->ChunkListIds.size()); ++index) {
            YCHECK(outputStatisticsMap.find(index) != outputStatisticsMap.end());
            auto outputStatistics = outputStatisticsMap[index];
            if (outputStatistics.chunk_count() == 0) {
                Controller->ChunkListPool->Reinstall(joblet->ChunkListIds[index]);
                joblet->ChunkListIds[index] = NullChunkListId;
            }
        }

        auto inputStatistics = GetTotalInputDataStatistics(statistics);
        auto outputStatistics = GetTotalOutputDataStatistics(statistics);
        // It's impossible to check row count preservation on interrupted job.
        if (Controller->IsRowCountPreserved() && jobSummary.InterruptReason == EInterruptReason::None) {
            LOG_ERROR_IF(inputStatistics.row_count() != outputStatistics.row_count(),
                "Input/output row count mismatch in completed job (Input: %v, Output: %v, Task: %v)",
                inputStatistics.row_count(),
                outputStatistics.row_count(),
                GetId());
            YCHECK(inputStatistics.row_count() == outputStatistics.row_count());
        }
    } else {
        auto& chunkListIds = joblet->ChunkListIds;
        Controller->ChunkListPool->Release(chunkListIds);
        std::fill(chunkListIds.begin(), chunkListIds.end(), NullChunkListId);
    }
    GetChunkPoolOutput()->Completed(joblet->OutputCookie, jobSummary);

    Controller->RegisterStderr(joblet, jobSummary);
    Controller->RegisterCores(joblet, jobSummary);

    UpdateMaximumUsedTmpfsSize(statistics);
}

void TOperationControllerBase::TTask::ReinstallJob(TJobletPtr joblet, std::function<void()> releaseOutputCookie)
{
    Controller->RemoveValueFromEstimatedHistogram(joblet);
    Controller->ReleaseChunkLists(joblet->ChunkListIds);

    auto list = HasInputLocality()
        ? GetChunkPoolOutput()->GetStripeList(joblet->OutputCookie)
        : nullptr;

    releaseOutputCookie();

    if (HasInputLocality()) {
        for (const auto& stripe : list->Stripes) {
            Controller->AddTaskLocalityHint(this, stripe);
        }
    }
    AddPendingHint();
}

void TOperationControllerBase::TTask::OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary)
{
    Controller->RegisterStderr(joblet, jobSummary);
    Controller->RegisterCores(joblet, jobSummary);

    YCHECK(jobSummary.Statistics);
    UpdateMaximumUsedTmpfsSize(*jobSummary.Statistics);

    ReinstallJob(joblet, BIND([=] {GetChunkPoolOutput()->Failed(joblet->OutputCookie);}));
}

void TOperationControllerBase::TTask::OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary)
{
    if (joblet->StderrTableChunkListId) {
        Controller->ReleaseChunkLists({joblet->StderrTableChunkListId});
    }
    if (joblet->CoreTableChunkListId) {
        Controller->ReleaseChunkLists({joblet->CoreTableChunkListId});
    }

    ReinstallJob(joblet, BIND([=] {GetChunkPoolOutput()->Aborted(joblet->OutputCookie, jobSummary.AbortReason);}));
}

void TOperationControllerBase::TTask::OnJobLost(TCompletedJobPtr completedJob)
{
    YCHECK(LostJobCookieMap.insert(std::make_pair(
        completedJob->OutputCookie,
        completedJob->InputCookie)).second);
}

void TOperationControllerBase::TTask::OnTaskCompleted()
{
    LOG_DEBUG("Task completed");
}

bool TOperationControllerBase::TTask::CanScheduleJob(
    ISchedulingContext* /*context*/,
    const TJobResources& /*jobLimits*/)
{
    return true;
}

void TOperationControllerBase::TTask::DoCheckResourceDemandSanity(
    const TJobResources& neededResources)
{
    if (Controller->ShouldSkipSanityCheck()) {
        return;
    }

    if (!Dominates(*Controller->CachedMaxAvailableExecNodeResources_, neededResources)) {
        // It seems nobody can satisfy the demand.
        Controller->OnOperationFailed(
            TError("No online node can satisfy the resource demand")
                << TErrorAttribute("task", GetId())
                << TErrorAttribute("needed_resources", neededResources));
    }
}

void TOperationControllerBase::TTask::CheckResourceDemandSanity(
    const TJobResources& nodeResourceLimits,
    const TJobResources& neededResources)
{
    // The task is requesting more than some node is willing to provide it.
    // Maybe it's OK and we should wait for some time.
    // Or maybe it's not and the task is requesting something no one is able to provide.

    // First check if this very node has enough resources (including those currently
    // allocated by other jobs).
    if (Dominates(nodeResourceLimits, neededResources)) {
        return;
    }

    // Schedule check in controller thread.
    Controller->GetCancelableInvoker()->Invoke(BIND(
        &TTask::DoCheckResourceDemandSanity,
        MakeWeak(this),
        neededResources));
}

void TOperationControllerBase::TTask::AddPendingHint()
{
    Controller->AddTaskPendingHint(this);
}

void TOperationControllerBase::TTask::AddLocalityHint(TNodeId nodeId)
{
    Controller->AddTaskLocalityHint(this, nodeId);
}

std::unique_ptr<TNodeDirectoryBuilder> TOperationControllerBase::TTask::MakeNodeDirectoryBuilder(
    TSchedulerJobSpecExt* schedulerJobSpec)
{
    return Controller->OperationType == EOperationType::RemoteCopy
        ? std::make_unique<TNodeDirectoryBuilder>(
            Controller->InputNodeDirectory,
            schedulerJobSpec->mutable_input_node_directory())
        : nullptr;
}

void TOperationControllerBase::TTask::AddSequentialInputSpec(
    TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    auto directoryBuilder = MakeNodeDirectoryBuilder(schedulerJobSpecExt);
    auto* inputSpec = schedulerJobSpecExt->add_input_table_specs();
    const auto& list = joblet->InputStripeList;
    for (const auto& stripe : list->Stripes) {
        AddChunksToInputSpec(directoryBuilder.get(), inputSpec, stripe);
    }
    UpdateInputSpecTotals(jobSpec, joblet);
}

void TOperationControllerBase::TTask::AddParallelInputSpec(
    TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    auto directoryBuilder = MakeNodeDirectoryBuilder(schedulerJobSpecExt);
    const auto& list = joblet->InputStripeList;
    for (const auto& stripe : list->Stripes) {
        auto* inputSpec = stripe->Foreign
            ? schedulerJobSpecExt->add_foreign_input_table_specs()
            : schedulerJobSpecExt->add_input_table_specs();
        AddChunksToInputSpec(directoryBuilder.get(), inputSpec, stripe);
    }
    UpdateInputSpecTotals(jobSpec, joblet);
}

void TOperationControllerBase::TTask::AddChunksToInputSpec(
    TNodeDirectoryBuilder* directoryBuilder,
    TTableInputSpec* inputSpec,
    TChunkStripePtr stripe)
{
    for (const auto& dataSlice : stripe->DataSlices) {
        inputSpec->add_chunk_spec_count_per_data_slice(dataSlice->ChunkSlices.size());
        for (const auto& chunkSlice : dataSlice->ChunkSlices) {
            auto newChunkSpec = inputSpec->add_chunk_specs();
            ToProto(newChunkSpec, chunkSlice, dataSlice->Type);
            if (dataSlice->Tag) {
                newChunkSpec->set_data_slice_tag(*dataSlice->Tag);
            }

            if (directoryBuilder) {
                auto replicas = chunkSlice->GetInputChunk()->GetReplicaList();
                directoryBuilder->Add(replicas);
            }
        }
    }

    if (inputSpec->chunk_specs_size() > 0) {
        // Make spec incompatible with older nodes.
        ToProto(inputSpec->add_data_slice_descriptors(), GetIncompatibleDataSliceDescriptor());
    }
}

void TOperationControllerBase::TTask::UpdateInputSpecTotals(
    TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    const auto& list = joblet->InputStripeList;
    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    schedulerJobSpecExt->set_input_data_weight(
        schedulerJobSpecExt->input_data_weight() +
        list->TotalDataWeight);
    schedulerJobSpecExt->set_input_row_count(
        schedulerJobSpecExt->input_row_count() +
        list->TotalRowCount);
}

void TOperationControllerBase::TTask::AddFinalOutputSpecs(
    TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    YCHECK(joblet->ChunkListIds.size() == Controller->OutputTables.size());
    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    for (int index = 0; index < Controller->OutputTables.size(); ++index) {
        const auto& table = Controller->OutputTables[index];
        auto* outputSpec = schedulerJobSpecExt->add_output_table_specs();
        outputSpec->set_table_writer_options(ConvertToYsonString(table.Options).GetData());
        if (table.WriterConfig) {
            outputSpec->set_table_writer_config(table.WriterConfig.GetData());
        }
        outputSpec->set_timestamp(table.Timestamp);
        ToProto(outputSpec->mutable_table_schema(), table.TableUploadOptions.TableSchema);
        ToProto(outputSpec->mutable_chunk_list_id(), joblet->ChunkListIds[index]);
    }
}

void TOperationControllerBase::TTask::AddIntermediateOutputSpec(
    TJobSpec* jobSpec,
    TJobletPtr joblet,
    const TKeyColumns& keyColumns)
{
    YCHECK(joblet->ChunkListIds.size() == 1);
    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    auto* outputSpec = schedulerJobSpecExt->add_output_table_specs();

    auto options = New<NTableClient::TTableWriterOptions>();
    options->Account = Controller->Spec->IntermediateDataAccount;
    options->ChunksVital = false;
    options->ChunksMovable = false;
    options->ReplicationFactor = Controller->Spec->IntermediateDataReplicationFactor;
    options->MediumName = Controller->Spec->IntermediateDataMediumName;
    options->CompressionCodec = Controller->Spec->IntermediateCompressionCodec;
    // Distribute intermediate chunks uniformly across storage locations.
    options->PlacementId = Controller->OperationId;

    outputSpec->set_table_writer_options(ConvertToYsonString(options).GetData());

    ToProto(outputSpec->mutable_table_schema(), TTableSchema::FromKeyColumns(keyColumns));
    ToProto(outputSpec->mutable_chunk_list_id(), joblet->ChunkListIds[0]);
}

void TOperationControllerBase::TTask::ResetCachedMinNeededResources()
{
    CachedMinNeededResources.Reset();
}

TJobResources TOperationControllerBase::TTask::ApplyMemoryReserve(const TExtendedJobResources& jobResources) const
{
    TJobResources result;
    result.SetCpu(jobResources.GetCpu());
    result.SetUserSlots(jobResources.GetUserSlots());
    i64 memory = jobResources.GetFootprintMemory();
    memory += jobResources.GetJobProxyMemory() * Controller->GetJobProxyMemoryDigest(GetJobType())->GetQuantile(Controller->Config->JobProxyMemoryReserveQuantile);
    if (GetUserJobSpec()) {
        memory += jobResources.GetUserJobMemory() * Controller->GetUserJobMemoryDigest(GetJobType())->GetQuantile(Controller->Config->UserJobMemoryReserveQuantile);
    } else {
        YCHECK(jobResources.GetUserJobMemory() == 0);
    }
    result.SetMemory(memory);
    result.SetNetwork(jobResources.GetNetwork());
    return result;
}

void TOperationControllerBase::TTask::UpdateMaximumUsedTmpfsSize(const NJobTrackerClient::TStatistics& statistics) {
    auto maxUsedTmpfsSize = FindNumericValue(
        statistics,
        "/user_job/max_tmpfs_size");

    if (!maxUsedTmpfsSize) {
        return;
    }

    if (!MaximumUsedTmfpsSize || *MaximumUsedTmfpsSize < *maxUsedTmpfsSize) {
        MaximumUsedTmfpsSize = *maxUsedTmpfsSize;
    }
}

void TOperationControllerBase::TTask::AddFootprintAndUserJobResources(TExtendedJobResources& jobResources) const
{
    jobResources.SetFootprintMemory(GetFootprintMemorySize());
    auto userJobSpec = GetUserJobSpec();
    if (userJobSpec) {
        jobResources.SetUserJobMemory(userJobSpec->MemoryLimit);
    }
}

TJobResources TOperationControllerBase::TTask::GetMinNeededResources() const
{
    if (!CachedMinNeededResources) {
        YCHECK(GetPendingJobCount() > 0);
        CachedMinNeededResources = GetMinNeededResourcesHeavy();
    }
    auto result = ApplyMemoryReserve(*CachedMinNeededResources);
    if (result.GetUserSlots() > 0 && result.GetMemory() == 0) {
        LOG_WARNING("Found min needed resources of task with non-zero user slots and zero memory");
    }
    return result;
}

void TOperationControllerBase::TTask::RegisterIntermediate(
    TJobletPtr joblet,
    TChunkStripePtr stripe,
    TTaskPtr destinationTask,
    bool attachToLivePreview)
{
    RegisterIntermediate(
        joblet,
        stripe,
        destinationTask->GetChunkPoolInput(),
        attachToLivePreview);

    if (destinationTask->HasInputLocality()) {
        Controller->AddTaskLocalityHint(destinationTask, stripe);
    }
    destinationTask->AddPendingHint();
}

void TOperationControllerBase::TTask::RegisterIntermediate(
    TJobletPtr joblet,
    TChunkStripePtr stripe,
    IChunkPoolInput* destinationPool,
    bool attachToLivePreview)
{
    IChunkPoolInput::TCookie inputCookie;

    auto lostIt = LostJobCookieMap.find(joblet->OutputCookie);
    if (lostIt == LostJobCookieMap.end()) {
        inputCookie = destinationPool->Add(stripe);
    } else {
        inputCookie = lostIt->second;
        destinationPool->Resume(inputCookie, stripe);
        LostJobCookieMap.erase(lostIt);
    }

    // Store recovery info.
    auto completedJob = New<TCompletedJob>(
        joblet->JobId,
        this,
        joblet->OutputCookie,
        joblet->InputStripeList->TotalDataWeight,
        destinationPool,
        inputCookie,
        joblet->NodeDescriptor);

    Controller->RegisterIntermediate(
        joblet,
        completedJob,
        stripe,
        attachToLivePreview);
}

TChunkStripePtr TOperationControllerBase::TTask::BuildIntermediateChunkStripe(
    google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>* chunkSpecs)
{
    auto stripe = New<TChunkStripe>();

    i64 currentTableRowIndex = 0;
    for (int index = 0; index < chunkSpecs->size(); ++index) {
        auto inputChunk = New<TInputChunk>(std::move(*chunkSpecs->Mutable(index)));
        // NB(max42): Having correct table row indices on intermediate data is important for
        // some chunk pools. This affects the correctness of sort operation with sorted
        // merge phase over several intermediate chunks.
        inputChunk->SetTableRowIndex(currentTableRowIndex);
        currentTableRowIndex += inputChunk->GetRowCount();
        auto chunkSlice = CreateInputChunkSlice(std::move(inputChunk));
        auto dataSlice = CreateUnversionedInputDataSlice(std::move(chunkSlice));
        // NB(max42): This heavily relies on the property of intermediate data being deterministic
        // (i.e. it may be reproduced with exactly the same content divided into chunks with exactly
        // the same boundary keys when the job output is lost).
        dataSlice->Tag = index;
        stripe->DataSlices.emplace_back(std::move(dataSlice));
    }
    return stripe;
}

void TOperationControllerBase::TTask::RegisterOutput(
    TJobletPtr joblet,
    TOutputChunkTreeKey key,
    const TCompletedJobSummary& jobSummary)
{
    Controller->RegisterOutput(joblet, key, jobSummary);
}
