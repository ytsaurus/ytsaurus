#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>
#include <yt/yt/flow/library/cpp/controller/resource_instances.h>

namespace NYT::NFlow::NController {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TResourceLayoutStorage
    : public TPersistedStateStorageHandlerBase<std::string>
{
public:
    void Select(TSequenceId, std::vector<TStorageRow>&) override
    { }

    void Execute(std::vector<TStorageRow>&&, const std::vector<TSequenceId>&, bool, const std::vector<TPersistedStateCommitContext*>&) override
    { }
};

class TResourceInstancesTest
    : public ::testing::Test
{
protected:
    TFlowViewPtr View_ = New<TFlowView>();
    NFlow::TWorkerPtr Worker_ = New<NFlow::TWorker>();

    void SetUp() override
    {
        auto control = New<TPersistedStateControl<std::string>>(New<TResourceLayoutStorage>());
        View_->State->AttachToControl(control);
        control->Recover();
        View_->State->ExecutionSpec->PipelineState->TrySetValue(EPipelineState::Working, TestVersionProvider());
        Worker_->RpcAddress = "worker";
        Worker_->IncarnationId = TIncarnationId(TGuid::Create());
        View_->State->Workers.emplace(Worker_->RpcAddress, Worker_);
        auto spec = New<TPipelineSpec>();
        auto computation = New<TComputationSpec>();
        computation->RequiredResourceIds.emplace(TResourceId("resource"), New<TResourceDescription>());
        spec->Computations.emplace(TComputationId("computation"), computation);
        spec->Resources.emplace(TResourceId("resource"), New<TResourceSpec>());
        View_->State->ExecutionSpec->PipelineSpec->TrySetValue(spec, TestVersionProvider());
        View_->State->StartMutation();
    }

    TResourceInstancePtr Instance()
    {
        return View_->State->ExecutionSpec->Layout->ResourceInstances.begin()->second;
    }

    TWorkerStatusPtr Status()
    {
        auto status = New<TWorkerStatus>();
        status->WorkerIncarnationId = Worker_->IncarnationId;
        View_->Feedback->WorkerStatuses[Worker_->RpcAddress] = status;
        return status;
    }
};

TEST_F(TResourceInstancesTest, StableAssignmentAndNewWorkerIncarnation)
{
    EnsureResourceInstances(View_);
    ASSERT_EQ(View_->State->ExecutionSpec->Layout->ResourceInstances.size(), 1u);
    auto first = Instance();
    EnsureResourceInstances(View_);
    EXPECT_EQ(Instance()->ResourceInstanceId, first->ResourceInstanceId);
    Worker_->IncarnationId = TIncarnationId(TGuid::Create());
    EnsureResourceInstances(View_);
    EXPECT_EQ(View_->State->ExecutionSpec->Layout->ResourceInstances.size(), 1u);
    EXPECT_NE(Instance()->ResourceInstanceId, first->ResourceInstanceId);
}

TEST_F(TResourceInstancesTest, PreloadPlacementAndInactivePipeline)
{
    auto spec = View_->State->ExecutionSpec->PipelineSpec->GetValue();
    spec->Resources.at(TResourceId("resource"))->PreloadRequired = true;
    EnsureResourceInstances(View_);
    EXPECT_EQ(View_->State->ExecutionSpec->Layout->ResourceInstances.size(), 0u);
    auto workerSpec = New<TWorkerSpec>();
    workerSpec->WorkerIncarnationId = Worker_->IncarnationId;
    workerSpec->PreloadResources.insert(TResourceId("resource"));
    View_->State->ExecutionSpec->Layout->WorkerSpecs.emplace(Worker_->RpcAddress, workerSpec);
    EnsureResourceInstances(View_);
    EXPECT_EQ(View_->State->ExecutionSpec->Layout->ResourceInstances.size(), 1u);
    View_->State->ExecutionSpec->Layout->ResourceInstances.erase(Instance()->ResourceInstanceId);
    View_->State->ExecutionSpec->PipelineState->TrySetValue(EPipelineState::Stopped, TestVersionProvider());
    EnsureResourceInstances(View_);
    EXPECT_EQ(View_->State->ExecutionSpec->Layout->ResourceInstances.size(), 0u);
}

TEST_F(TResourceInstancesTest, DependenciesRespectPreloadPlacementAndRunningJobs)
{
    auto spec = View_->State->ExecutionSpec->PipelineSpec->GetValue();
    auto dependency = New<TResourceSpec>();
    dependency->PreloadRequired = true;
    spec->Resources.emplace(TResourceId("dependency"), dependency);
    auto intermediate = New<TResourceSpec>();
    intermediate->Dependencies.emplace(TResourceId("dependency"), New<TResourceDescription>());
    spec->Resources.emplace(TResourceId("intermediate"), intermediate);
    auto root = spec->Resources.at(TResourceId("resource"));
    root->AlwaysOn = true;
    root->Dependencies.emplace(TResourceId("intermediate"), New<TResourceDescription>());
    auto otherWorker = New<NFlow::TWorker>();
    otherWorker->RpcAddress = "other";
    otherWorker->IncarnationId = TIncarnationId(TGuid::Create());
    View_->State->Workers.emplace(otherWorker->RpcAddress, otherWorker);
    const auto& layout = View_->State->ExecutionSpec->Layout;

    EnsureResourceInstances(View_);
    EXPECT_EQ(layout->ResourceInstances.size(), 0u);
    auto workerSpec = New<TWorkerSpec>();
    workerSpec->WorkerIncarnationId = TIncarnationId(TGuid::Create());
    workerSpec->PreloadResources.insert(TResourceId("dependency"));
    layout->WorkerSpecs.emplace(Worker_->RpcAddress, workerSpec);
    EnsureResourceInstances(View_);
    EXPECT_EQ(layout->ResourceInstances.size(), 0u);

    workerSpec->WorkerIncarnationId = Worker_->IncarnationId;
    EnsureResourceInstances(View_);
    ASSERT_EQ(layout->ResourceInstances.size(), 3u);
    THashSet<TResourceInstanceId> originalIds;
    for (const auto& [instanceId, instance] : layout->ResourceInstances) {
        EXPECT_EQ(instance->WorkerAddress, Worker_->RpcAddress);
        originalIds.insert(instanceId);
    }
    auto partition = New<TPartition>();
    partition->PartitionId = TPartitionId(TGuid::Create());
    partition->ComputationId = TComputationId("computation");
    layout->Partitions.emplace(partition->PartitionId, partition);
    auto job = New<TJob>();
    job->JobId = TJobId(TGuid::Create());
    job->PartitionId = partition->PartitionId;
    job->WorkerAddress = Worker_->RpcAddress;
    job->WorkerIncarnationId = Worker_->IncarnationId;
    layout->Jobs.emplace(job->JobId, job);
    workerSpec->PreloadResources.clear();
    EnsureResourceInstances(View_);
    ASSERT_EQ(layout->ResourceInstances.size(), originalIds.size());
    for (auto id : originalIds) {
        EXPECT_TRUE(layout->ResourceInstances.contains(id));
    }

    layout->Jobs.erase(job->JobId);
    EnsureResourceInstances(View_);
    EXPECT_EQ(layout->ResourceInstances.size(), 0u);
}

TEST_F(TResourceInstancesTest, RetiredInstanceCannotSatisfySuccessorReadiness)
{
    EnsureResourceInstances(View_);
    auto instance = Instance();
    auto workerSpec = New<TWorkerSpec>();
    workerSpec->PreloadResources.insert(instance->ResourceId);
    View_->State->ExecutionSpec->Layout->WorkerSpecs.emplace(Worker_->RpcAddress, workerSpec);
    auto status = Status();
    auto oldStatus = New<TWorkerResourceStatus>();
    oldStatus->ResourceId = instance->ResourceId;
    oldStatus->LoadState = EResourceLoadState::Loaded;
    status->ResourceInstanceStatuses.emplace(TResourceInstanceId(TGuid::Create()), oldStatus);
    auto current = New<TWorkerResourceStatus>();
    current->ResourceId = instance->ResourceId;
    current->LoadState = EResourceLoadState::Loading;
    status->ResourceInstanceStatuses.emplace(instance->ResourceInstanceId, current);
    ProjectResourceInstanceStatuses(View_);
    auto projected = View_->Feedback->WorkerStatuses.at(Worker_->RpcAddress);
    EXPECT_EQ(projected->ResourceStatuses.size(), 1u);
    EXPECT_EQ(projected->ResourceStatuses.at(instance->ResourceId)->LoadState, EResourceLoadState::Loading);
    EXPECT_EQ(projected->PreloadedResourceStates.at(instance->ResourceId), EPreloadedResourceState::Preloading);
}

TEST_F(TResourceInstancesTest, IdleAssignmentStaysVisibleWithoutJoiningRevisionStatus)
{
    auto idleWorker = New<NFlow::TWorker>();
    idleWorker->RpcAddress = "idle-worker";
    idleWorker->IncarnationId = TIncarnationId(TGuid::Create());
    View_->State->Workers.emplace(idleWorker->RpcAddress, idleWorker);
    EnsureResourceInstances(View_);
    const auto& layout = View_->State->ExecutionSpec->Layout;
    ASSERT_EQ(layout->ResourceInstances.size(), 2u);

    auto loadedStatus = Status();
    auto idleStatus = New<TWorkerStatus>();
    idleStatus->WorkerIncarnationId = idleWorker->IncarnationId;
    View_->Feedback->WorkerStatuses.emplace(idleWorker->RpcAddress, idleStatus);
    const auto resourceId = TResourceId("resource");
    TResourceInstanceId idleInstanceId;
    for (const auto& [instanceId, instance] : layout->ResourceInstances) {
        auto status = New<TWorkerResourceStatus>();
        status->ResourceId = resourceId;
        status->ResourceInstanceId = instanceId;
        status->ResourceIncarnationGeneration = 1;
        status->TargetRevisionId = 42;
        if (instance->WorkerAddress == Worker_->RpcAddress) {
            status->LoadState = EResourceLoadState::Loaded;
            status->AppliedRevisionId = 42;
            loadedStatus->ResourceInstanceStatuses.emplace(instanceId, status);
        } else {
            status->LoadState = EResourceLoadState::NotLoaded;
            idleStatus->ResourceInstanceStatuses.emplace(instanceId, status);
            idleInstanceId = instanceId;
        }
    }
    ASSERT_NE(idleInstanceId, TResourceInstanceId{});

    ProjectResourceInstanceStatuses(View_);
    const auto& statuses = View_->Feedback->WorkerStatuses;
    auto loaded = GetOrCrash(statuses, Worker_->RpcAddress);
    ASSERT_EQ(loaded->ResourceStatuses.size(), 1u);
    EXPECT_EQ(GetOrCrash(loaded->ResourceStatuses, resourceId)->AppliedRevisionId, 42);
    auto idle = GetOrCrash(statuses, idleWorker->RpcAddress);
    EXPECT_TRUE(idle->ResourceStatuses.empty());
    ASSERT_EQ(idle->ResourceInstanceStatuses.size(), 1u);
    EXPECT_EQ(GetOrCrash(idle->ResourceInstanceStatuses, idleInstanceId)->LoadState, EResourceLoadState::NotLoaded);
    EXPECT_EQ(layout->ResourceInstances.size(), 2u);

    for (auto state : {EResourceLoadState::Loading, EResourceLoadState::Loaded, EResourceLoadState::Failed}) {
        idle = GetOrCrash(statuses, idleWorker->RpcAddress);
        GetOrCrash(idle->ResourceInstanceStatuses, idleInstanceId)->LoadState = state;
        ProjectResourceInstanceStatuses(View_);
        idle = GetOrCrash(statuses, idleWorker->RpcAddress);
        ASSERT_EQ(idle->ResourceStatuses.size(), 1u);
        EXPECT_EQ(GetOrCrash(idle->ResourceStatuses, resourceId)->LoadState, state);
    }
}

class TResourceRetryTest
    : public TResourceInstancesTest
    , public ::testing::WithParamInterface<std::pair<bool, bool>>
{ };

TEST_P(TResourceRetryTest, FailedResourceGetsNewIdAfterDependentJobsLeave)
{
    const auto [preload, alwaysOn] = GetParam();
    auto spec = View_->State->ExecutionSpec->PipelineSpec->GetValue();
    auto dependency = New<TResourceSpec>();
    spec->Resources.emplace(TResourceId("dependency"), dependency);
    spec->Resources.at(TResourceId("resource"))->Dependencies.emplace(TResourceId("dependency"), New<TResourceDescription>());
    spec->Resources.at(TResourceId("resource"))->AlwaysOn = alwaysOn;
    if (preload) {
        auto workerSpec = New<TWorkerSpec>();
        workerSpec->WorkerIncarnationId = Worker_->IncarnationId;
        workerSpec->PreloadResources.insert(TResourceId("resource"));
        View_->State->ExecutionSpec->Layout->WorkerSpecs.emplace(Worker_->RpcAddress, workerSpec);
    }
    spec->Computations.emplace(TComputationId("unrelated"), New<TComputationSpec>());
    EnsureResourceInstances(View_);
    TResourceInstancePtr root;
    TResourceInstanceId dependencyId;
    for (const auto& [_, instance] : View_->State->ExecutionSpec->Layout->ResourceInstances) {
        if (instance->ResourceId == TResourceId("resource")) {
            root = instance;
        } else if (instance->ResourceId == TResourceId("dependency")) {
            dependencyId = instance->ResourceInstanceId;
        }
    }
    ASSERT_TRUE(root);
    ASSERT_NE(dependencyId, TResourceInstanceId{});
    auto status = Status();
    auto failure = New<TWorkerResourceStatus>();
    failure->LoadState = EResourceLoadState::Failed;
    failure->ResourceId = TResourceId("dependency");
    status->ResourceInstanceStatuses.emplace(dependencyId, failure);
    auto job = New<TJob>();
    job->JobId = TJobId(TGuid::Create());
    job->WorkerAddress = Worker_->RpcAddress;
    job->WorkerIncarnationId = Worker_->IncarnationId;
    auto partition = New<TPartition>();
    partition->PartitionId = TPartitionId(TGuid::Create());
    partition->ComputationId = alwaysOn ? TComputationId("unrelated") : TComputationId("computation");
    job->PartitionId = partition->PartitionId;
    View_->State->ExecutionSpec->Layout->Partitions.emplace(partition->PartitionId, partition);
    const auto& layout = View_->State->ExecutionSpec->Layout;
    layout->Jobs.emplace(job->JobId, job);
    EnsureResourceInstances(View_);
    EXPECT_TRUE(layout->ResourceInstances.contains(root->ResourceInstanceId));
    EXPECT_TRUE(layout->ResourceInstances.contains(dependencyId));
    layout->Jobs.erase(job->JobId);
    EnsureResourceInstances(View_);
    EXPECT_FALSE(layout->ResourceInstances.contains(root->ResourceInstanceId));
    EXPECT_FALSE(layout->ResourceInstances.contains(dependencyId));
    EXPECT_EQ(layout->ResourceInstances.size(), 2u);
    THashSet<TResourceInstanceId> replacementIds;
    for (const auto& [instanceId, instance] : layout->ResourceInstances) {
        replacementIds.insert(instanceId);
        EXPECT_EQ(instance->WorkerAddress, Worker_->RpcAddress);
        EXPECT_EQ(instance->WorkerIncarnationId, Worker_->IncarnationId);
    }
    EnsureResourceInstances(View_);
    for (auto instanceId : replacementIds) {
        EXPECT_TRUE(layout->ResourceInstances.contains(instanceId));
    }
    status->ResourceInstanceStatuses.clear();
    for (const auto& [instanceId, instance] : layout->ResourceInstances) {
        auto loaded = New<TWorkerResourceStatus>();
        loaded->ResourceId = instance->ResourceId;
        loaded->LoadState = EResourceLoadState::Loaded;
        status->ResourceInstanceStatuses.emplace(instanceId, loaded);
    }
    ProjectResourceInstanceStatuses(View_);
    if (preload) {
        EXPECT_EQ(View_->Feedback->WorkerStatuses.at(Worker_->RpcAddress)->PreloadedResourceStates.at(root->ResourceId),
            EPreloadedResourceState::Preloaded);
    }
}

TEST_F(TResourceInstancesTest, FailedDependenciesAreReplacedOnlyOnAffectedWorkers)
{
    auto spec = View_->State->ExecutionSpec->PipelineSpec->GetValue();
    spec->Resources.emplace(TResourceId("dependency"), New<TResourceSpec>());
    spec->Resources.at(TResourceId("resource"))->Dependencies.emplace(TResourceId("dependency"), New<TResourceDescription>());
    View_->State->Workers.clear();
    for (int index = 0; index < 2000; ++index) {
        auto worker = New<NFlow::TWorker>();
        worker->RpcAddress = Format("worker-%v", index);
        worker->IncarnationId = TIncarnationId(TGuid::Create());
        View_->State->Workers.emplace(worker->RpcAddress, worker);
        if (index % 2 == 0) {
            auto status = New<TWorkerStatus>();
            status->WorkerIncarnationId = worker->IncarnationId;
            View_->Feedback->WorkerStatuses.emplace(worker->RpcAddress, status);
        }
    }
    EnsureResourceInstances(View_);
    auto& instances = View_->State->ExecutionSpec->Layout->ResourceInstances;
    ASSERT_EQ(instances.size(), 4000u);
    THashMap<TResourceInstanceId, TResourceInstancePtr> originalInstances;
    for (const auto& [instanceId, instance] : instances) {
        originalInstances.emplace(instanceId, instance);
        auto status = View_->Feedback->WorkerStatuses.find(instance->WorkerAddress);
        if (instance->ResourceId == TResourceId("dependency") && status != View_->Feedback->WorkerStatuses.end()) {
            auto failure = New<TWorkerResourceStatus>();
            failure->ResourceId = instance->ResourceId;
            failure->LoadState = EResourceLoadState::Failed;
            status->second->ResourceInstanceStatuses.emplace(instanceId, failure);
        }
    }
    EnsureResourceInstances(View_);
    EXPECT_EQ(instances.size(), 4000u);
    for (const auto& [instanceId, instance] : originalInstances) {
        EXPECT_EQ(instances.contains(instanceId), !View_->Feedback->WorkerStatuses.contains(instance->WorkerAddress));
    }
}

INSTANTIATE_TEST_SUITE_P(
    LoadModes,
    TResourceRetryTest,
    ::testing::Values(std::pair(false, false), std::pair(true, false), std::pair(false, true)));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NController
