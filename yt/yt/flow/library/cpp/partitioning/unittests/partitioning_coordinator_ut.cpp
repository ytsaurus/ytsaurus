#include <yt/yt/flow/library/cpp/partitioning/partitioning_coordinator.h>

#include <yt/yt/flow/library/cpp/client/public.h>

#include <yt/yt/flow/library/cpp/common/persisted_state_control.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <util/stream/file.h>

#include <util/system/tempfile.h>

namespace NYT::NFlow::NPartitioning {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TRangeController
    : public IComputationController
{
public:
    struct TRangeParameters
        : public TParameters
        , public TPartitioningSpec
    {
        REGISTER_YSON_STRUCT(TRangeParameters);

        static void Register(TRegistrar)
        { }
    };

    TIntrusivePtr<TRangeParameters> StaticPartitioningSpec = New<TRangeParameters>();
    TDynamicPartitioningSpecPtr PartitioningSpec = New<TDynamicPartitioningSpec>();
    TVersion SinkTopologyVersion;

    TParametersPtr GetParametersBase() const override
    {
        return StaticPartitioningSpec;
    }

    TDynamicParametersPtr GetDynamicParametersBase() const override
    {
        return {};
    }

    void Init(IInitContextPtr) override
    { }

    void Sync() override
    { }

    void Commit() override
    { }

    TProcessPartitionTraverseDataResultPtr ProcessPartitionTraverseData(
        const THashMap<TPartitionId, TNodeTraverseDataPtr>&,
        const TNodeTraverseDataPtr&,
        const TFlowViewPtr&) override
    {
        YT_ABORT();
    }

    TPartitioningTopology DescribePartitioningTopology() override
    {
        return {.Value = TPartitioningTopology::TRange{}};
    }

    TPartitioningDescription DescribePartitioning(const TPartitioningStatus&) override
    {
        return {
            .Value = TPartitioningDescription::TRange{
                .PartitioningSpec = StaticPartitioningSpec,
                .DynamicPartitioningSpec = PartitioningSpec,
                .SinkTopologyVersion = SinkTopologyVersion,
            },
        };
    }

    double ComputePartitionWeight(const TPartitionId&, const TFlowViewPtr&) override
    {
        return 1;
    }

    void UpdateWatermarkState(TWatermarkStatePtr) override
    { }
};

DEFINE_REFCOUNTED_TYPE(TRangeController);

class TPartitioningTransactionTest
    : public ::testing::Test
{
protected:
    const TComputationId ComputationId_{"consumer"};
    const TStreamId Input_{"input"};
    TInstant Now_ = TInstant::Seconds(1'789'400'000);
    TStateManagerMockPtr Storage_ = New<TStateManagerMock>();
    TIntrusivePtr<TRangeController> Controller_ = New<TRangeController>();
    TPipelineSpecPtr Spec_ = New<TPipelineSpec>();
    TFlowViewPtr FlowView_ = New<TFlowView>();
    NProfiling::TSolomonRegistryPtr Registry_ = New<NProfiling::TSolomonRegistry>();
    TPartitioningCoordinatorPtr Coordinator_;
    TJobStatusPtr Job_ = New<TJobStatus>();

    void SetUp() override
    {
        Registry_->SetWindowSize(12);
        auto spec = New<TComputationSpec>();
        spec->InputStreamIds.insert(Input_);
        spec->GroupBySchema = New<NTableClient::TTableSchema>(
            std::vector{
                NTableClient::TColumnSchema("hash", NTableClient::EValueType::Uint64)
                    .SetRequired(true)
                    .SetSortOrder(NTableClient::ESortOrder::Ascending),
            });
        Spec_->Computations[ComputationId_] = spec;
        Controller_->PartitioningSpec->MinPartitionCount = 1;
        Controller_->PartitioningSpec->MaxPartitionCount = 1;
        Controller_->PartitioningSpec->PartitionCountDoubleDelay = TDuration::Seconds(1);

        auto saved = New<TPartitioningCoordinatorState>();
        auto state = New<TComputationPartitioningState>();
        saved->Computations[ComputationId_] = state;
        Storage_->Set("/v1", ConvertToYsonString(saved));
        CreateCoordinator();

        auto control = New<TPersistedStateControl<std::string>>(New<TNullStorageHandler<std::string>>());
        FlowView_->State->AttachToControl(control);
        control->Recover();
        FlowView_->State->ExecutionSpec->PipelineSpec->TrySetValue(Spec_, TestVersionProvider());
        auto dynamicSpec = New<TDynamicPipelineSpec>();
        dynamicSpec->JobManager->Partitioning->EnableThroughputAutoscaler = true;
        FlowView_->State->ExecutionSpec->DynamicPipelineSpec->TrySetValue(dynamicSpec, TestVersionProvider());
        FlowView_->State->ExecutionSpec->PipelineState->TrySetValue(EPipelineState::Working, TestVersionProvider());
        auto partition = New<TPartition>();
        partition->ComputationId = ComputationId_;
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->CurrentJobId = TJobId(TGuid::Create());
        partition->LowerKey = MinKey();
        partition->UpperKey = MaxKey();
        partition->State = EPartitionState::Executing;
        FlowView_->State->StartMutation();
        FlowView_->State->ExecutionSpec->Layout->Partitions.emplace(partition->PartitionId, partition);
        auto job = New<TJob>();
        job->JobId = *partition->CurrentJobId;
        job->PartitionId = partition->PartitionId;
        FlowView_->State->ExecutionSpec->Layout->Jobs.emplace(job->JobId, job);
        FlowView_->State->CommitMutation();
        Job_->JobId = *partition->CurrentJobId;
        Job_->InitedTime = Now_;
        Job_->FromPartitionTraverseData = New<TFromPartitionTraverseData>();
        auto stream = New<TStreamTraverseData>();
        stream->InflightMetrics = New<TInflightMetrics>();
        stream->InflightMetrics->ReadyCount = 0;
        stream->InflightMetrics->OfferedCountPerSec = 10;
        stream->InflightMetrics->ProcessedCountPerSec = 10;
        Job_->FromPartitionTraverseData->Node->Streams[Input_] = stream;
        auto status = New<TPartitionJobStatus>();
        status->CurrentJobId = partition->CurrentJobId;
        status->CurrentJobStatus = Job_;
        FlowView_->Feedback->PartitionJobStatuses[partition->PartitionId] = status;

        for (int minute = 0; minute <= 6; ++minute) {
            Publish(200);
            Coordinator_->BeginIteration(FlowView_);
            RunPass();
            Coordinator_->Commit();
            Now_ += TDuration::Minutes(1);
        }
        ASSERT_EQ(ReadState()->ThroughputAutoscaler.ObservedPartitionCount, 1);
    }

    void CreateCoordinator()
    {
        Coordinator_.Reset();
        Coordinator_ = New<TPartitioningCoordinator>(
            THashMap<TComputationId, IComputationControllerPtr>{{ComputationId_, Controller_}},
            Spec_,
            Storage_->CreateContext(),
            NLogging::TLogger("test"),
            NLogging::TLogger("test"),
            NProfiling::TProfiler(Registry_, "/partitioning"));
    }

    double ReadSensor(TStringBuf name, NProfiling::TTagList tags = {})
    {
        tags.emplace_back("computation_id", ComputationId_.Underlying());
        return ConvertTo<double>(BuildYsonStringFluently().Do([&] (TFluentAny fluent) {
            Registry_->ReadRecentSensorValues(Format("yt%v", name), tags, {}, fluent);
        }));
    }

    TComputationPartitioningStatePtr ReadState()
    {
        Coordinator_->Sync();
        Storage_->Sync();
        return GetOrCrash(ConvertTo<TPartitioningCoordinatorStatePtr>(Storage_->Get("/v1"))->Computations, ComputationId_);
    }

    void Publish(double capacity)
    {
        auto observation = New<TProcessingObservation>();
        observation->Sequence = Job_->ProcessingObservation ? Job_->ProcessingObservation->Sequence + 1 : 1;
        observation->CapturedAt = Now_;
        observation->SpecGeneration = FlowView_->State->ExecutionSpec->GetEpoch();
        const auto& previous = Job_->ProcessingObservation;
        auto elapsed = previous ? Now_ - previous->CapturedAt : TDuration::Seconds(1);
        i64 count = static_cast<i64>(capacity * elapsed.SecondsFloat());
        observation->ProcessedCount = (previous ? previous->ProcessedCount : 0) + count;
        observation->ProcessedByteSize = observation->ProcessedCount * 10;
        observation->ProcessingTime = (previous ? previous->ProcessingTime : TDuration::Zero()) + elapsed;
        Job_->ProcessingObservation = observation;
    }

    TDynamicKeyCountPartitioningSpecPtr EnableKeyCount(i64 maxKeys = 1, i64 maxPartitions = 8)
    {
        auto dynamic = CloneYsonStruct(FlowView_->State->ExecutionSpec->DynamicPipelineSpec->GetValue());
        auto computation = New<TDynamicComputationSpec>();
        const auto& config = Controller_->StaticPartitioningSpec->KeyCountPartitioning;
        config->Enable = true;
        config->MaxKeysPerPartition = maxKeys;
        dynamic->Computations[ComputationId_] = computation;
        FlowView_->State->ExecutionSpec->DynamicPipelineSpec->TrySetValue(dynamic, TestVersionProvider());
        Controller_->PartitioningSpec->MaxPartitionCount = maxPartitions;
        return Controller_->PartitioningSpec->KeyCountPartitioning;
    }

    void PublishKeys(int count, bool terminal = false)
    {
        Job_->ObservedKeys.emplace();
        for (int key = 1; key <= count; ++key) {
            Job_->ObservedKeys->emplace(MakeKey(static_cast<ui64>(key)), Now_);
        }
        Job_->IsFinished = terminal;
        Job_->Error = terminal ? TError(EErrorCode::PartitionKeyLimitExceeded, "Key limit") : TError();
    }

    void RunPass()
    {
        FlowView_->State->CurrentTimestamp = TSystemTimestamp(Now_.Seconds());
        FlowView_->Feedback->UpdateTime = Now_;
        Job_->Epoch = FlowView_->State->ExecutionSpec->GetEpoch();
        Job_->UpdateTime = Now_;
        for (auto& [_, status] : FlowView_->Feedback->PartitionJobStatuses) {
            status->CurrentJobStatusUpdateTime = Now_;
        }
        FlowView_->State->StartMutation();
        Coordinator_->DoPartitioning(FlowView_);
        FlowView_->State->CommitMutation();
    }
};

TEST_F(TPartitioningTransactionTest, KeyCountHealthyReportsDoNotReshape)
{
    EnableKeyCount(6);
    Controller_->PartitioningSpec->DesiredPartitionCount = 8;
    PublishKeys(7);
    const auto original = FlowView_->State->ExecutionSpec->Layout->Partitions.begin()->second;
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_EQ(FlowView_->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    EXPECT_EQ(FlowView_->State->ExecutionSpec->Layout->Partitions.begin()->second->CurrentJobId, original->CurrentJobId);
    EXPECT_EQ(ReadState()->KeyCountPartitioning->ObservedKeys.size(), 7u);
}

TEST_F(TPartitioningTransactionTest, KeyCountFailureUsesHistoryAndBalancedRanges)
{
    EnableKeyCount(6);
    PublishKeys(6);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    PublishKeys(7, true);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    std::vector<TKeyRange> ranges;
    for (const auto& [_, partition] : FlowView_->State->ExecutionSpec->Layout->Partitions) {
        if (partition->State == EPartitionState::Executing) {
            ranges.push_back({*partition->LowerKey, *partition->UpperKey});
        }
    }
    std::sort(ranges.begin(), ranges.end());
    EXPECT_EQ(ranges, (std::vector<TKeyRange>{{MinKey(), MakeKey(5u)}, {MakeKey(5u), MaxKey()}}));
    const auto state = ReadState()->KeyCountPartitioning;
    EXPECT_EQ(state->ObservedKeys.size(), 7u);
}

TEST_F(TPartitioningTransactionTest, KeyCountCapacityHaltsJobsAndDynamicIncreaseRecovers)
{
    EnableKeyCount(1, 1);
    PublishKeys(7, true);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_EQ(CountExecutableKeyCountOwners(FlowView_, ComputationId_), 0);
    EXPECT_TRUE(FlowView_->EphemeralState->KeyCountPartitioningBlockedComputations.contains(ComputationId_));
    EnableKeyCount(1, 8);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_FALSE(FlowView_->EphemeralState->KeyCountPartitioningBlockedComputations.contains(ComputationId_));
    int executing = 0;
    for (const auto& [_, partition] : FlowView_->State->ExecutionSpec->Layout->Partitions) {
        executing += partition->State == EPartitionState::Executing;
    }
    EXPECT_EQ(executing, 7);
}

TEST_F(TPartitioningTransactionTest, KeyCountEmptyLayoutRecoversAfterCapacityIncrease)
{
    EnableKeyCount(1, 1);
    PublishKeys(7, true);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    ASSERT_TRUE(FlowView_->EphemeralState->KeyCountPartitioningBlockedComputations.contains(ComputationId_));
    ASSERT_EQ(ReadState()->KeyCountPartitioning->ObservedKeys.size(), 7u);

    const auto& layout = FlowView_->State->ExecutionSpec->Layout;
    std::vector<TPartitionId> ids;
    for (const auto& [id, _] : layout->Partitions) {
        ids.push_back(id);
    }
    FlowView_->State->StartMutation();
    for (const auto& id : ids) {
        layout->RemovePartition(id);
    }
    FlowView_->State->CommitMutation();
    CreateCoordinator();
    EnableKeyCount(1, 8);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();

    EXPECT_FALSE(FlowView_->EphemeralState->KeyCountPartitioningBlockedComputations.contains(ComputationId_));
    EXPECT_EQ(layout->Partitions.size(), 7u);
}

TEST_F(TPartitioningTransactionTest, KeyCountInheritedOwnerExcessHaltsAndRecoversWithSameCaps)
{
    EnableKeyCount(6, 2);
    PublishKeys(1);
    const auto& layout = FlowView_->State->ExecutionSpec->Layout;
    FlowView_->State->StartMutation();
    auto retiring = New<TPartition>();
    retiring->PartitionId = TPartitionId(TGuid::Create());
    retiring->ComputationId = ComputationId_;
    retiring->State = EPartitionState::Interrupting;
    retiring->StateTimestamp = Now_;
    retiring->LowerKey = MinKey();
    retiring->UpperKey = MaxKey();
    layout->CreatePartition(retiring);
    auto job = New<TJob>();
    job->JobId = TJobId(TGuid::Create());
    job->PartitionId = retiring->PartitionId;
    layout->CreateJob(job);
    FlowView_->State->CommitMutation();
    EnableKeyCount(6, 1);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_TRUE(FlowView_->EphemeralState->KeyCountPartitioningBlockedComputations.contains(ComputationId_));
    EXPECT_EQ(layout->Jobs.size(), 0u);
    // A finished owner is metadata, not an executable owner consuming the cap.
    FlowView_->State->StartMutation();
    layout->CreateJob(job);
    FlowView_->State->CommitMutation();
    auto terminal = New<TPartitionJobStatus>();
    terminal->CurrentJobStatus = New<TJobStatus>();
    terminal->CurrentJobStatus->JobId = job->JobId;
    terminal->CurrentJobStatus->IsFinished = true;
    FlowView_->Feedback->PartitionJobStatuses[retiring->PartitionId] = terminal;
    // The committed stop was fenced before publication; retiring metadata can remain.
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_FALSE(FlowView_->EphemeralState->KeyCountPartitioningBlockedComputations.contains(ComputationId_));
}

TEST_F(TPartitioningTransactionTest, KeyCountCapacityRecoversWhenKeysExpire)
{
    EnableKeyCount(1, 1)->KeyRetentionPeriod = TDuration::Seconds(10);
    PublishKeys(2, true);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    ASSERT_TRUE(FlowView_->EphemeralState->KeyCountPartitioningBlockedComputations.contains(ComputationId_));

    Now_ += TDuration::Seconds(11);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_FALSE(FlowView_->EphemeralState->KeyCountPartitioningBlockedComputations.contains(ComputationId_));
    EXPECT_TRUE(ReadState()->KeyCountPartitioning->ObservedKeys.empty());
}

TEST_F(TPartitioningTransactionTest, KeyCountCatalogueSurvivesRetryAndRestartDoesNotAgeDowntime)
{
    auto config = EnableKeyCount(6);
    config->KeyRetentionPeriod = TDuration::Seconds(10);
    PublishKeys(6);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    EXPECT_EQ(ReadState()->KeyCountPartitioning->ObservedKeys.size(), 6u);
    Job_->ObservedKeys.reset();
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    EXPECT_EQ(ReadState()->KeyCountPartitioning->ObservedKeys.size(), 6u);
    Coordinator_->Commit();
    PublishKeys(6);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    const auto before = ReadState()->KeyCountPartitioning->ObservedKeys;
    Now_ += TDuration::Days(2);
    CreateCoordinator();
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_EQ(ReadState()->KeyCountPartitioning->ObservedKeys, before);
}

TEST_F(TPartitioningTransactionTest, KeyCountLocalSplitRetainsHealthyNeighborIdentity)
{
    EnableKeyCount(1);
    const auto layout = FlowView_->State->ExecutionSpec->Layout;
    const auto original = layout->Partitions.begin()->second;
    FlowView_->State->StartMutation();
    auto narrowed = CloneYsonStruct(original);
    narrowed->UpperKey = MakeKey(10u);
    layout->Partitions.insert_or_assign(original->PartitionId, narrowed);
    auto neighbor = CloneYsonStruct(original);
    neighbor->PartitionId = TPartitionId(TGuid::Create());
    neighbor->CurrentJobId = TJobId(TGuid::Create());
    neighbor->LowerKey = MakeKey(10u);
    layout->Partitions.emplace(neighbor->PartitionId, neighbor);
    auto job = New<TJob>();
    job->JobId = *neighbor->CurrentJobId;
    job->PartitionId = neighbor->PartitionId;
    layout->Jobs.emplace(job->JobId, job);
    FlowView_->State->CommitMutation();
    PublishKeys(3, true);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_EQ(GetOrCrash(layout->Partitions, neighbor->PartitionId)->State, EPartitionState::Executing);
    EXPECT_EQ(GetOrCrash(layout->Partitions, neighbor->PartitionId)->CurrentJobId, neighbor->CurrentJobId);
    EXPECT_EQ(ReadState()->KeyCountPartitioning->ObservedKeys.size(), 3u);
}

TEST_F(TPartitioningTransactionTest, KeyCountHistoryUnionUsesFiveFourFour)
{
    EnableKeyCount(6);
    PublishKeys(6);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    PublishKeys(13, true);
    for (int key = 1; key <= 6; ++key) {
        Job_->ObservedKeys->erase(MakeKey(static_cast<ui64>(key)));
    }
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    std::vector<TKeyRange> ranges;
    for (const auto& [_, partition] : FlowView_->State->ExecutionSpec->Layout->Partitions) {
        if (partition->State == EPartitionState::Executing) {
            ranges.push_back({*partition->LowerKey, *partition->UpperKey});
        }
    }
    std::sort(ranges.begin(), ranges.end());
    EXPECT_EQ(ranges, (std::vector<TKeyRange>{{MinKey(), MakeKey(6u)}, {MakeKey(6u), MakeKey(10u)}, {MakeKey(10u), MaxKey()}}));
}

TEST_F(TPartitioningTransactionTest, KeyCountPeriodicRebalanceRecreatesOnlyWhenSpreadExceedsOne)
{
    EnableKeyCount(6)->RebalancePeriod = TDuration::Seconds(10);
    const auto& layout = FlowView_->State->ExecutionSpec->Layout;
    const auto original = layout->Partitions.begin()->second;
    FlowView_->State->StartMutation();
    auto first = CloneYsonStruct(original);
    first->UpperKey = MakeKey(5u);
    layout->Partitions.insert_or_assign(first->PartitionId, first);
    auto second = CloneYsonStruct(original);
    second->PartitionId = TPartitionId(TGuid::Create());
    second->CurrentJobId.reset();
    second->StateTimestamp = Now_;
    second->LowerKey = MakeKey(5u);
    layout->CreatePartition(second);
    FlowView_->State->CommitMutation();
    PublishKeys(7);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    Now_ += TDuration::Seconds(11);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_EQ(layout->Partitions.size(), 2u);
    EXPECT_EQ(GetOrCrash(layout->Partitions, first->PartitionId)->CurrentJobId, original->CurrentJobId);

    PublishKeys(10);
    Now_ += TDuration::Seconds(11);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    std::vector<TKeyRange> ranges;
    for (const auto& [_, partition] : layout->Partitions) {
        if (partition->State == EPartitionState::Executing) {
            ranges.push_back({*partition->LowerKey, *partition->UpperKey});
        }
    }
    std::sort(ranges.begin(), ranges.end());
    EXPECT_EQ(ranges, (std::vector<TKeyRange>{{MinKey(), MakeKey(6u)}, {MakeKey(6u), MaxKey()}}));
    EXPECT_EQ(GetOrCrash(layout->Partitions, first->PartitionId)->State, EPartitionState::Interrupting);
    EXPECT_EQ(GetOrCrash(layout->Partitions, second->PartitionId)->State, EPartitionState::Interrupting);
}

TEST_F(TPartitioningTransactionTest, KeyCountDisabledIgnoresObservations)
{
    PublishKeys(7);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_TRUE(ReadState()->KeyCountPartitioning->ObservedKeys.empty());
}

TEST_F(TPartitioningTransactionTest, KeyCountPeriodicRebalanceRetriesAfterAbortedIteration)
{
    EnableKeyCount(6)->RebalancePeriod = TDuration::Seconds(10);
    const auto& layout = FlowView_->State->ExecutionSpec->Layout;
    FlowView_->State->StartMutation();
    auto first = CloneYsonStruct(layout->Partitions.begin()->second);
    first->UpperKey = MakeKey(7u);
    layout->Partitions.insert_or_assign(first->PartitionId, first);
    auto second = CloneYsonStruct(first);
    second->PartitionId = TPartitionId(TGuid::Create());
    second->CurrentJobId.reset();
    second->StateTimestamp = Now_;
    second->LowerKey = MakeKey(7u);
    second->UpperKey = MaxKey();
    layout->CreatePartition(second);
    FlowView_->State->CommitMutation();
    PublishKeys(6);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();

    Now_ += TDuration::Seconds(11);
    const auto committed = FlowView_;
    FlowView_ = committed->CopyPtr();
    FlowView_->State = committed->State->Clone();
    FlowView_->Feedback->UpdateTime = Now_;
    Coordinator_->BeginIteration(committed);
    FlowView_->State->StartMutation();
    Coordinator_->DoPartitioning(FlowView_);
    EXPECT_EQ(GetOrCrash(FlowView_->State->ExecutionSpec->Layout->Partitions, first->PartitionId)->State,
        EPartitionState::Interrupting);
    FlowView_ = committed;
    ASSERT_EQ(GetOrCrash(layout->Partitions, first->PartitionId)->State, EPartitionState::Executing);

    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_EQ(GetOrCrash(layout->Partitions, first->PartitionId)->State, EPartitionState::Interrupting);
    EXPECT_EQ(GetOrCrash(layout->Partitions, second->PartitionId)->State, EPartitionState::Interrupting);
}

TEST_F(TPartitioningTransactionTest, KeyCountPeriodicRebalanceRecoversAfterLoweringPartitionLimit)
{
    EnableKeyCount(6, 3)->RebalancePeriod = TDuration::Seconds(10);
    const auto& layout = FlowView_->State->ExecutionSpec->Layout;
    FlowView_->State->StartMutation();
    auto first = CloneYsonStruct(layout->Partitions.begin()->second);
    first->UpperKey = MakeKey(2u);
    layout->Partitions.insert_or_assign(first->PartitionId, first);
    for (ui64 key = 2; key <= 3; ++key) {
        auto partition = CloneYsonStruct(first);
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->CurrentJobId.reset();
        partition->StateTimestamp = Now_;
        partition->LowerKey = MakeKey(key);
        partition->UpperKey = key == 3 ? MaxKey() : MakeKey(key + 1);
        layout->CreatePartition(partition);
    }
    FlowView_->State->CommitMutation();
    PublishKeys(3);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    Controller_->PartitioningSpec->MaxPartitionCount = 1;
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    ASSERT_TRUE(FlowView_->EphemeralState->KeyCountPartitioningBlockedComputations.contains(ComputationId_));

    Now_ += TDuration::Seconds(11);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_FALSE(FlowView_->EphemeralState->KeyCountPartitioningBlockedComputations.contains(ComputationId_));
    std::vector<TKeyRange> ranges;
    for (const auto& [_, partition] : layout->Partitions) {
        if (partition->State == EPartitionState::Executing) {
            ranges.push_back({*partition->LowerKey, *partition->UpperKey});
        }
    }
    EXPECT_EQ(ranges, std::vector{UniversalKeyRange()});
}

TEST_F(TPartitioningTransactionTest, KeyCountPauseResumeGivesExpiryGraceAndThenExpiresOldKeys)
{
    EnableKeyCount(6)->KeyRetentionPeriod = TDuration::Seconds(10);
    PublishKeys(6);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    FlowView_->State->ExecutionSpec->PipelineState->TrySetValue(EPipelineState::Paused, TestVersionProvider());
    PublishKeys(7);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    Now_ += TDuration::Days(1);
    FlowView_->State->ExecutionSpec->PipelineState->TrySetValue(EPipelineState::Working, TestVersionProvider());
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_EQ(ReadState()->KeyCountPartitioning->ObservedKeys.size(), 7u);
    Now_ += TDuration::Seconds(11);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_TRUE(ReadState()->KeyCountPartitioning->ObservedKeys.empty());
}

TEST_F(TPartitioningTransactionTest, KeyCountPauseAndRestartDoNotResurrectExpiredObservations)
{
    EnableKeyCount(6)->KeyRetentionPeriod = TDuration::Seconds(10);
    PublishKeys(2);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();

    Now_ += TDuration::Seconds(11);
    GetOrCrash(*Job_->ObservedKeys, MakeKey(2u)) = Now_;
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    const THashMap<TKey, TInstant> expected{{MakeKey(2u), Now_}};
    ASSERT_EQ(ReadState()->KeyCountPartitioning->ObservedKeys, expected);

    FlowView_->State->ExecutionSpec->PipelineState->TrySetValue(EPipelineState::Paused, TestVersionProvider());
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_EQ(ReadState()->KeyCountPartitioning->ObservedKeys, expected);

    Now_ += TDuration::Days(1);
    CreateCoordinator();
    FlowView_->State->ExecutionSpec->PipelineState->TrySetValue(EPipelineState::Working, TestVersionProvider());
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_EQ(ReadState()->KeyCountPartitioning->ObservedKeys, expected);

    GetOrCrash(*Job_->ObservedKeys, MakeKey(1u)) = Now_;
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_EQ(ReadState()->KeyCountPartitioning->ObservedKeys.size(), 2u);
}

TEST_F(TPartitioningTransactionTest, KeyCountLifetimeOverflowRecreatesJobWhenOldKeysHaveExpired)
{
    EnableKeyCount(1)->KeyRetentionPeriod = TDuration::Seconds(10);
    PublishKeys(1);
    const auto original = FlowView_->State->ExecutionSpec->Layout->Partitions.begin()->second;
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    Now_ += TDuration::Seconds(11);
    PublishKeys(2, true);
    (*Job_->ObservedKeys)[MakeKey(1u)] = Now_ - TDuration::Seconds(11);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    const auto& layout = FlowView_->State->ExecutionSpec->Layout;
    EXPECT_EQ(GetOrCrash(layout->Partitions, original->PartitionId)->State, EPartitionState::Interrupting);
    EXPECT_FALSE(GetOrCrash(layout->Partitions, original->PartitionId)->CurrentJobId);
    EXPECT_EQ(ReadState()->KeyCountPartitioning->ObservedKeys.size(), 1u);
}

TEST_F(TPartitioningTransactionTest, KeyCountOrdinaryFailureDoesNotRepartition)
{
    EnableKeyCount(1);
    PublishKeys(3);
    Job_->IsFinished = true;
    Job_->Error = TError("Ordinary failure");
    const auto original = FlowView_->State->ExecutionSpec->Layout->Partitions.begin()->second;
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_EQ(FlowView_->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    EXPECT_EQ(GetOrCrash(FlowView_->State->ExecutionSpec->Layout->Partitions, original->PartitionId)->CurrentJobId, original->CurrentJobId);

    FlowView_->State->StartMutation();
    FlowView_->State->ExecutionSpec->Layout->RemoveJob(Job_->JobId, EJobFinishReason::Failed);
    FlowView_->State->CommitMutation();
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    EXPECT_EQ(FlowView_->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    const auto& partition = GetOrCrash(FlowView_->State->ExecutionSpec->Layout->Partitions, original->PartitionId);
    EXPECT_EQ(partition->State, EPartitionState::Executing);
    EXPECT_EQ(partition->LowerKey, MinKey());
    EXPECT_EQ(partition->UpperKey, MaxKey());
}

TEST_F(TPartitioningTransactionTest, CommittedDemandPeakSurvivesCoordinatorRestart)
{
    const auto saved = ReadState();
    ASSERT_TRUE(saved->ThroughputDemandHistory);
    Coordinator_.Reset();
    CreateCoordinator();
    Job_->FromPartitionTraverseData->Node->Streams[Input_]->InflightMetrics->OfferedCountPerSec = 1;
    Now_ += TDuration::Days(1);
    for (int minute = 0; minute <= 6; ++minute) {
        Publish(200);
        Coordinator_->BeginIteration(FlowView_);
        RunPass();
        Coordinator_->Commit();
        Now_ += TDuration::Minutes(1);
    }
    Registry_->Collect();
    EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/throughput/peak_steady_demand"), 10);
}

TEST_F(TPartitioningTransactionTest, MetricsSurvivePassesAndRetainDecisionCounts)
{
    Job_->PerformanceMetrics = New<TNodePerformanceMetrics>();
    Job_->PerformanceMetrics->CpuUsage10m = 0.2;
    Controller_->PartitioningSpec->DesiredAveragePartitionCpuLoad = 0.1;
    for (int pass = 1; pass <= 2; ++pass) {
        Publish(200);
        Coordinator_->BeginIteration(FlowView_);
        RunPass();
        Coordinator_->Commit();
        Registry_->Collect();
        EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/throughput/capacity"), 200);
        EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/throughput/capacity_known"), 1);
        EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/criterion_average", {{"criterion", "cpu"}}), 0.2);
        EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/criterion_proposed_partition_count", {{"criterion", "cpu"}}), 2);
        EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/proposed_partition_count"), 1);
        EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/decision_count", {{"criterion", "cpu"}}), pass);
        Now_ += TDuration::Minutes(1);
    }
    auto config = FlowView_->State->ExecutionSpec->DynamicPipelineSpec->GetValue()->JobManager->Partitioning;
    config->EnableThroughputAutoscaler = false;
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    Registry_->Collect();
    EXPECT_THROW(ReadSensor("/partitioning/throughput/capacity"), std::exception);
    EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/decision_count", {{"criterion", "cpu"}}), 3);
}

TEST_F(TPartitioningTransactionTest, ManualPartitionCountRemovesThroughputTelemetry)
{
    Publish(200);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    Registry_->Collect();
    EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/throughput/capacity_known"), 1);

    Controller_->PartitioningSpec->DesiredPartitionCount = 1;
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    Registry_->Collect();
    EXPECT_THROW(ReadSensor("/partitioning/throughput/capacity"), std::exception);
    EXPECT_THROW(ReadSensor("/partitioning/throughput/capacity_known"), std::exception);
}

TEST_F(TPartitioningTransactionTest, RepartitionCounterSurvivesCollectionAndCountsOnlyCommits)
{
    Controller_->SinkTopologyVersion = TVersion(42);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    Controller_->SinkTopologyVersion = TVersion(43);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Registry_->Collect();
    EXPECT_THROW(ReadSensor("/partitioning/repartition_count", {{"initiator", "sink_topology"}}), std::exception);
    Coordinator_->Commit();
    Registry_->Collect();
    EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/repartition_count", {{"initiator", "sink_topology"}}), 1);
    Coordinator_->Commit();
    Registry_->Collect();
    EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/repartition_count", {{"initiator", "sink_topology"}}), 1);
}

TEST_F(TPartitioningTransactionTest, ThroughputIgnoresLegacyThroughputCriteria)
{
    Controller_->PartitioningSpec->MaxPartitionCount = 64;
    Controller_->PartitioningSpec->DesiredAveragePartitionMessagesPerSecond = 1;
    Controller_->PartitioningSpec->DesiredAveragePartitionBytesPerSecond = 1;
    Job_->InputMetrics = New<TNodeInputMetrics>();
    Job_->InputMetrics->Global.MessagesPerSecond = 1'000'000;
    Job_->InputMetrics->Global.BytesPerSecond = 1'000'000'000;
    for (int minute = 0; minute < 10; ++minute) {
        Publish(200);
        Coordinator_->BeginIteration(FlowView_);
        RunPass();
        Coordinator_->Commit();
        Now_ += TDuration::Minutes(1);
    }
    EXPECT_EQ(FlowView_->State->ExecutionSpec->Layout->Partitions.size(), 1u);
}

TEST_F(TPartitioningTransactionTest, CandidateJobRemovalDoesNotChangeCommittedMeasurement)
{
    Controller_->PartitioningSpec->MaxPartitionCount = 64;
    Job_->FromPartitionTraverseData->Node->Streams[Input_]->InflightMetrics->OfferedCountPerSec = 500;
    for (int minute = 0; minute < 3; ++minute) {
        Publish(200);
        Coordinator_->BeginIteration(FlowView_);
        RunPass();
        Coordinator_->Commit();
        Now_ += TDuration::Minutes(1);
    }
    Publish(200);
    Job_->UpdateTime = Now_;
    FlowView_->Feedback->UpdateTime = Now_;
    auto candidate = FlowView_->CopyPtr();
    candidate->State = candidate->State->Clone();
    Coordinator_->BeginIteration(FlowView_);
    candidate->State->StartMutation();
    auto partition = CloneYsonStruct(candidate->State->ExecutionSpec->Layout->Partitions.begin()->second);
    partition->CurrentJobId.reset();
    partition->StateEpoch = candidate->State->ExecutionSpec->GetEpoch();
    partition->StateTimestamp = Now_;
    candidate->State->ExecutionSpec->Layout->UpdatePartition(partition);
    Coordinator_->DoPartitioning(candidate);
    const auto proposed = ReadState()->ThroughputAutoscaler;
    ASSERT_TRUE(proposed.Anchor);
    EXPECT_EQ(proposed.Anchor->Capacity, 200);

    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    const auto retried = ReadState()->ThroughputAutoscaler;
    ASSERT_TRUE(retried.Anchor);
    EXPECT_EQ(retried.Anchor->Capacity, proposed.Anchor->Capacity);
    EXPECT_TRUE(retried.PendingPartitionCount);
}

TEST_F(TPartitioningTransactionTest, RepeatedPassCannotReplaceRollbackBoundary)
{
    const i64 committed = ReadState()->ThroughputAutoscaler.PipelineResetGeneration;
    auto config = FlowView_->State->ExecutionSpec->DynamicPipelineSpec->GetValue()->JobManager->Partitioning->Throughput;
    config->ResetGeneration = 1;
    Publish(200);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    config->ResetGeneration = 2;
    Now_ += TDuration::Seconds(1);
    Publish(200);
    RunPass();
    EXPECT_EQ(ReadState()->ThroughputAutoscaler.PipelineResetGeneration, 2);

    Coordinator_->BeginIteration(FlowView_);
    EXPECT_EQ(ReadState()->ThroughputAutoscaler.PipelineResetGeneration, committed);
    RunPass();
    EXPECT_EQ(ReadState()->ThroughputAutoscaler.PipelineResetGeneration, 2);
    RunPass();
    EXPECT_EQ(ReadState()->ThroughputAutoscaler.PipelineResetGeneration, 2);
    Coordinator_->Commit();
    Coordinator_->BeginIteration(FlowView_);
    EXPECT_EQ(ReadState()->ThroughputAutoscaler.PipelineResetGeneration, 2);
    EXPECT_TRUE(ReadState()->ThroughputAutoscaler.ResetRequested);
}

TEST_F(TPartitioningTransactionTest, RollbackPreservesExistingSinkTopologyBoundary)
{
    Controller_->SinkTopologyVersion = TVersion(42);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    EXPECT_EQ(ReadState()->LastAppliedSinkTopologyVersion, TVersion(42));

    Coordinator_->BeginIteration(FlowView_);
    EXPECT_FALSE(ReadState()->LastAppliedSinkTopologyVersion);
    RunPass();
    Coordinator_->Commit();
    Coordinator_->BeginIteration(FlowView_);
    EXPECT_EQ(ReadState()->LastAppliedSinkTopologyVersion, TVersion(42));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NPartitioning
