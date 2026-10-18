#include <yt/yt/flow/library/cpp/common/persisted_state_control.h>

#include <yt/yt/flow/library/cpp/throughput_autoscaler/processing_meter.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NFlow {
namespace {

TEST(TProcessingMeterTest, SourceContributionsAreSummedWithoutExtrapolation)
{
    const TComputationId computationId("source");
    const TStreamId first("first");
    const TStreamId second("second");
    auto flowView = New<TFlowView>();
    auto control = New<TPersistedStateControl<std::string>>(New<TNullStorageHandler<std::string>>());
    flowView->State->AttachToControl(control);
    control->Recover();
    flowView->State->StartMutation();
    std::vector<TJobStatusPtr> jobs;
    std::vector<TPartitionId> partitions;
    for (int index = 0; index < 3; ++index) {
        auto partition = New<TPartition>();
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->ComputationId = computationId;
        partition->State = EPartitionState::Executing;
        partition->CurrentJobId = TJobId(TGuid::Create());
        flowView->State->ExecutionSpec->Layout->Partitions.emplace(partition->PartitionId, partition);
        auto job = New<TJobStatus>();
        job->JobId = *partition->CurrentJobId;
        job->InitedTime = TInstant::Seconds(1);
        job->UpdateTime = TInstant::Seconds(100);
        job->ProcessingObservation = New<TProcessingObservation>();
        job->ProcessingObservation->Sequence = 1;
        job->ProcessingObservation->CapturedAt = TInstant::Seconds(100);
        job->FromPartitionTraverseData = New<TFromPartitionTraverseData>();
        for (const auto& streamId : {first, second}) {
            auto stream = New<TStreamTraverseData>();
            stream->InflightMetrics = New<TInflightMetrics>();
            const bool active = (streamId == first) == (index == 0);
            stream->InflightMetrics->ReadyCount = active ? 20 : 0;
            stream->InflightMetrics->OfferedCountPerSec = active ? 10 : 0;
            job->FromPartitionTraverseData->Node->Streams[streamId] = stream;
        }
        auto status = New<TPartitionJobStatus>();
        status->CurrentJobId = partition->CurrentJobId;
        status->CurrentJobStatus = job;
        flowView->Feedback->PartitionJobStatuses[partition->PartitionId] = status;
        partitions.push_back(partition->PartitionId);
        jobs.push_back(job);
    }
    flowView->State->CommitMutation();
    flowView->Feedback->UpdateTime = TInstant::Seconds(100);
    auto spec = New<TComputationSpec>();
    spec->SourceStreams[first] = New<TSourceSpec>();
    spec->SourceStreams[second] = New<TSourceSpec>();
    TProcessingMeter meter;
    TProcessingMeter::TComputations computations{
        {
            computationId,
            {
                .Spec = spec,
                .Period = TDuration::Minutes(3),
            },
        },
    };
    const auto config = New<TThroughputSpec>();
    auto collect = [&] {
        return GetOrCrash(meter.Collect(flowView->State, flowView, config, computations), computationId);
    };
    auto sample = collect();
    EXPECT_EQ(GetOrCrash(sample.Streams, first).Offered, 10);
    EXPECT_EQ(GetOrCrash(sample.Streams, second).Offered, 20);
    EXPECT_EQ(GetOrCrash(sample.Streams, second).Ready, 40);

    jobs[1]->FromPartitionTraverseData->Node->Streams[second]->InflightMetrics->OfferedCountPerSec.reset();
    sample = collect();
    EXPECT_FALSE(GetOrCrash(sample.Streams, second).Offered);
    EXPECT_EQ(GetOrCrash(sample.Streams, second).Ready, 40);
    EXPECT_EQ(GetOrCrash(sample.Streams, first).Offered, 10);

    flowView->Feedback->PartitionJobStatuses.erase(partitions[0]);
    sample = collect();
    EXPECT_FALSE(sample.AllJobsHealthy);
    EXPECT_FALSE(GetOrCrash(sample.Streams, first).Offered);
    EXPECT_FALSE(GetOrCrash(sample.Streams, second).Ready);
}

} // namespace
} // namespace NYT::NFlow
