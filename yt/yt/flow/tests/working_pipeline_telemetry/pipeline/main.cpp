#include <yt/yt/flow/library/cpp/computation/meta_setter.h>
#include <yt/yt/flow/library/cpp/computation/stores/output_store.h>
#include <yt/yt/flow/library/cpp/computation/swift_map_computation.h>
#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_ordered_source_computation.h>

#include <yt/yt/flow/library/cpp/common/job_lineage_tracker.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/connectors/random/source.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/client/table_client/schema.h>

#include <cstdio>
#include <fstream>
#include <limits>

#include <fcntl.h>
#include <unistd.h>

using namespace NYT::NFlow;
using namespace NYT;
using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TSlowTelemetrySource
    : public TIntegerOffsetOrderedSourceBase
{
public:
    using TIntegerOffsetOrderedSourceBase::TIntegerOffsetOrderedSourceBase;
    using TSourceController = TRandomSourceController;

    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicRandomSourceParameters);

protected:
    const TTableSchemaPtr Schema_ = New<TTableSchema>(std::vector{
        TColumnSchema("key", EValueType::String),
        TColumnSchema("data", EValueType::String)});

    TFuture<std::vector<TRecord>> DoReadNextBatch(
        const TMessageBatcherSettingsPtr& /*settings*/,
        TOffset nextOffset,
        std::optional<TOffset> offsetLimit) override
    {
        if (offsetLimit && nextOffset >= *offsetLimit) {
            return MakeFuture(std::vector<TRecord>{});
        }
        // One ready row costs at least 200 ms of source I/O, independently of user processing.
        NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
        auto now = TSystemTimestamp(TInstant::Now().Seconds());
        auto payload = TPayload(TPayload::TUnderlying(2, 0, [] (TMutableUnversionedRow row) {
            row[0] = MakeUnversionedStringValue("", 0);
            row[1] = MakeUnversionedStringValue("", 1);
        }));
        return MakeFuture(std::vector<TRecord>{{.Offset = nextOffset,
            .WriteTimestamp = now,
            .CreateTimestamp = now,
            .Payloads = {std::move(payload)},
            .PayloadSchema = Schema_}});
    }

    void DoReportPersistedOffset(TOffset offset) override
    {
        UpdatePartitionInfo(TPartitionInfoUpdate{.CommittedOffsetExclusive = offset});
    }
};

YT_FLOW_DEFINE_SOURCE(TSlowTelemetrySource);

class TSingleBatchTelemetrySource
    : public TSlowTelemetrySource
{
public:
    using TSlowTelemetrySource::TSlowTelemetrySource;

private:
    bool Read_ = false;

    TFuture<std::vector<TRecord>> DoReadNextBatch(
        const TMessageBatcherSettingsPtr& settings,
        TOffset nextOffset,
        std::optional<TOffset> offsetLimit) override
    {
        if (std::exchange(Read_, true)) {
            return MakeFuture(std::vector<TRecord>{});
        }
        return TSlowTelemetrySource::DoReadNextBatch(settings, nextOffset, offsetLimit);
    }
};

YT_FLOW_DEFINE_SOURCE(TSingleBatchTelemetrySource);

////////////////////////////////////////////////////////////////////////////////

class TFilteringTelemetrySource
    : public TSlowTelemetrySource
{
public:
    using TSlowTelemetrySource::TSlowTelemetrySource;

private:
    TFuture<std::vector<TRecord>> DoReadNextBatch(
        const TMessageBatcherSettingsPtr&,
        TOffset nextOffset,
        std::optional<TOffset> offsetLimit) override
    {
        if (offsetLimit && nextOffset >= *offsetLimit) {
            return MakeFuture(std::vector<TRecord>{});
        }
        auto now = TSystemTimestamp(TInstant::Now().Seconds());
        std::vector<TPayload> payloads;
        for (int index = 0; index < 10; ++index) {
            const std::string data(index == 0 ? 8 : 4096, 'x');
            payloads.emplace_back(TPayload::TUnderlying(2, data.size() + 4, [index, &data] (TMutableUnversionedRow row) {
                row[0] = MakeUnversionedStringValue(index == 0 ? "keep" : "drop", 0);
                row[1] = MakeUnversionedStringValue(data, 1);
            }));
        }
        return MakeFuture(std::vector<TRecord>{{.Offset = nextOffset,
            .WriteTimestamp = now,
            .CreateTimestamp = now,
            .Payloads = std::move(payloads),
            .PayloadSchema = Schema_}});
    }
};

YT_FLOW_DEFINE_SOURCE(TFilteringTelemetrySource);

////////////////////////////////////////////////////////////////////////////////

class TRecordingLineageTracker
    : public IJobLineageTracker
{
public:
    TRecordingLineageTracker(IJobLineageTrackerPtr underlying, TStreamId output, TStreamId input, std::string path, std::function<void()> afterAddInputs = {})
        : Underlying_(std::move(underlying))
        , Output_(std::move(output))
        , Input_(std::move(input))
        , Path_(std::move(path))
        , AfterAddInputs_(std::move(afterAddInputs))
    { }

    void Add(TLineageDelta delta) override
    {
        TLineageDeltaValue observation;
        if (const auto* parents = delta.FindPtr(Output_)) {
            if (const auto* value = parents->FindPtr(Input_)) {
                observation = *value;
            }
        }
        Underlying_->Add(std::move(delta));
        if (observation.InputCount > 0) {
            const double average = observation.InputByteSize / observation.InputCount;
            MinimumInputBytes_ = std::min(MinimumInputBytes_, average);
            MaximumInputBytes_ = std::max(MaximumInputBytes_, average);
            const auto temporaryPath = Format("%v.input_bytes.tmp.%v", Path_, TGuid::Create());
            {
                std::ofstream output(temporaryPath);
                output.precision(17);
                output << MinimumInputBytes_ << ' ' << MaximumInputBytes_;
                output.close();
                YT_VERIFY(output.good());
            }
            const auto boundsPath = Path_ + ".input_bytes";
            YT_VERIFY(std::rename(temporaryPath.c_str(), boundsPath.c_str()) == 0);
        }
        if (!Reported_ && (observation.Count > 0 || observation.InputCount > 0)) {
            const auto temporaryPath = Format("%v.tmp.%v", Path_, TGuid::Create());
            {
                std::ofstream output(temporaryPath);
                output << observation.Count << ' ' << observation.InputCount << ' '
                       << observation.ByteSize << ' ' << observation.InputByteSize;
                output.close();
                YT_VERIFY(output.good());
            }
            YT_VERIFY(std::rename(temporaryPath.c_str(), Path_.c_str()) == 0);
            Reported_ = true;
        }
        Total_.Count += observation.Count;
        Total_.InputCount += observation.InputCount;
        Total_.ByteSize += observation.ByteSize;
        Total_.InputByteSize += observation.InputByteSize;
        const auto temporaryPath = Format("%v.totals.tmp.%v", Path_, TGuid::Create());
        {
            std::ofstream output(temporaryPath);
            output.precision(17);
            output << Total_.Count << ' ' << Total_.InputCount << ' '
                   << Total_.ByteSize << ' ' << Total_.InputByteSize;
            output.close();
            YT_VERIFY(output.good());
        }
        YT_VERIFY(std::rename(temporaryPath.c_str(), (Path_ + ".totals").c_str()) == 0);
        if (observation.InputCount > 0 && AfterAddInputs_) {
            AfterAddInputs_();
        }
    }

private:
    const IJobLineageTrackerPtr Underlying_;
    const TStreamId Output_;
    const TStreamId Input_;
    const std::string Path_;
    const std::function<void()> AfterAddInputs_;
    TLineageDeltaValue Total_;
    bool Reported_ = false;
    double MinimumInputBytes_ = std::numeric_limits<double>::infinity();
    double MaximumInputBytes_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

void WaitForCommitGate(const std::string& readyPath, const std::string& releasePath, const TNodeTraverseDataPtr& traverse, TPartitionId partitionId)
{
    if (!readyPath.empty() && !releasePath.empty()) {
        while (::access(releasePath.c_str(), F_OK) != 0) {
            if (::access(readyPath.c_str(), F_OK) != 0) {
                std::ofstream cycle(readyPath + ".cycle");
                cycle << ToString(partitionId) << ' ' << (traverse ? traverse->IterationCycle.value_or(-1) : -1);
                cycle.close();
                YT_VERIFY(cycle.good());
                if (int fd = ::open(readyPath.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0644); fd >= 0) {
                    ::close(fd);
                }
            }
            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TReaderParameters
    : public virtual TSwiftOrderedSourceComputation::TParameters
{
    TDuration ProcessingDelay;
    std::string LineageObservationPath;
    std::string PublicationReleasePath;
    std::optional<TSystemTimestamp> OutputEventTimestamp;
    std::string FailComment;
    bool FailBeforeCommit = false;
    std::string CommitGateReadyPath;
    std::string CommitGateReleasePath;

    REGISTER_YSON_STRUCT(TReaderParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("output_event_timestamp", &TThis::OutputEventTimestamp)
            .Optional();
        registrar.Parameter("publication_release_path", &TThis::PublicationReleasePath)
            .Default();
        registrar.Parameter("processing_delay", &TThis::ProcessingDelay)
            .Default();
        registrar.Parameter("lineage_observation_path", &TThis::LineageObservationPath)
            .Default();
        registrar.Parameter("fail_comment", &TThis::FailComment)
            .Default();
        registrar.Parameter("fail_before_commit", &TThis::FailBeforeCommit)
            .Default(false);
        registrar.Parameter("commit_gate_ready_path", &TThis::CommitGateReadyPath)
            .Default();
        registrar.Parameter("commit_gate_release_path", &TThis::CommitGateReleasePath)
            .Default();
    }
};

class TReader
    : public TSwiftOrderedSourceComputation
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TReaderParameters);

    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    static inline TStreamId OutputStreamId = TStreamId("data");

    void DoInit(IJobInitContextPtr context) override
    {
        TSwiftOrderedSourceComputation::DoInit(std::move(context));
        if (!GetParameters()->LineageObservationPath.empty()) {
            auto& tracker = GetContext()->JobLineageTracker;
            tracker = New<TRecordingLineageTracker>(tracker, OutputStreamId, TStreamId("random"), GetParameters()->LineageObservationPath);
        }
    }

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        if (!GetParameters()->FailComment.empty() && !GetParameters()->FailBeforeCommit) {
            THROW_ERROR_EXCEPTION("Failing intentionally with comment %Qv", GetParameters()->FailComment);
        }

        if (GetParameters()->ProcessingDelay) {
            NConcurrency::TDelayedExecutor::WaitForDuration(GetParameters()->ProcessingDelay);
        }
        auto builder = MakeOutputMessageBuilder(OutputStreamId);
        builder.Payload().SetValue(MakeUnversionedStringValue(GetColumnValue<TStringBuf>(message, "key")), "key");
        builder.Payload().SetValue(MakeUnversionedStringValue(GetColumnValue<TStringBuf>(message, "data")), "data");
        output->AddMessage(builder.Finish());
    }

    void DoSync(IRetryableTransactionPtr transaction) override
    {
        TSwiftOrderedSourceComputation::DoSync(transaction);

        const auto& readyPath = GetParameters()->CommitGateReadyPath;
        const auto& releasePath = GetParameters()->CommitGateReleasePath;
        WaitForCommitGate(readyPath, releasePath, GetStatus()->NodeTraverse, GetContext()->Partition->PartitionId);
        if (GetParameters()->FailBeforeCommit) {
            THROW_ERROR_EXCEPTION("Failing before commit with comment %Qv", GetParameters()->FailComment);
        }
    }
};

YT_FLOW_DEFINE_COMPUTATION(TReader);

////////////////////////////////////////////////////////////////////////////////

class TTransformReader
    : public TTransformOrderedSourceComputation
{
public:
    using TTransformOrderedSourceComputation::TTransformOrderedSourceComputation;
    YT_FLOW_EXTEND_PARAMETERS(TReaderParameters);

    void DoInit(IJobInitContextPtr context) override
    {
        TTransformOrderedSourceComputation::DoInit(std::move(context));
        if (!GetParameters()->LineageObservationPath.empty()) {
            auto& tracker = GetContext()->JobLineageTracker;
            tracker = New<TRecordingLineageTracker>(tracker, *GetSpec()->OutputStreamIds.begin(), TStreamId("random"), GetParameters()->LineageObservationPath);
        }
    }

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        if (!GetParameters()->FailComment.empty() && !GetParameters()->FailBeforeCommit) {
            THROW_ERROR_EXCEPTION("Failing intentionally with comment %Qv", GetParameters()->FailComment);
        }
        if (GetParameters()->ProcessingDelay) {
            NConcurrency::TDelayedExecutor::WaitForDuration(GetParameters()->ProcessingDelay);
        }
        auto builder = MakeOutputMessageBuilder(*GetSpec()->OutputStreamIds.begin());
        builder.Payload().SetValue(MakeUnversionedStringValue(GetColumnValue<TStringBuf>(message, "key")), "key");
        builder.Payload().SetValue(MakeUnversionedStringValue(GetColumnValue<TStringBuf>(message, "data")), "data");
        output->AddMessage(builder.Finish());
    }

    void DoSync(IRetryableTransactionPtr transaction) override
    {
        TTransformOrderedSourceComputation::DoSync(transaction);
        WaitForCommitGate(GetParameters()->CommitGateReadyPath, GetParameters()->CommitGateReleasePath, GetStatus()->NodeTraverse, GetContext()->Partition->PartitionId);
        if (GetParameters()->FailBeforeCommit) {
            THROW_ERROR_EXCEPTION("Failing before commit with comment %Qv", GetParameters()->FailComment);
        }
    }
};

YT_FLOW_DEFINE_COMPUTATION(TTransformReader);

class TDelayedReader
    : public TReader
{
public:
    using TReader::TReader;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        if (GetParameters()->ProcessingDelay) {
            NConcurrency::TDelayedExecutor::WaitForDuration(GetParameters()->ProcessingDelay);
        }
        auto builder = MakeOutputMessageBuilder(OutputStreamId);
        builder.Payload().SetValue(MakeUnversionedStringValue(GetColumnValue<TStringBuf>(message, "key")), "key");
        builder.Payload().SetValue(MakeUnversionedStringValue(GetColumnValue<TStringBuf>(message, "data")), "data");
        // Only the explicitly advanced test clock can release these candidates.
        builder.SetEventTimestamp(GetParameters()->OutputEventTimestamp.value_or(MaxAdequateTimestamp));
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TDelayedReader);

class TPublicationClockReader
    : public TTransformReader
{
public:
    using TTransformReader::TTransformReader;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        auto builder = MakeOutputMessageBuilder(*GetSpec()->OutputStreamIds.begin());
        builder.Payload().SetValue(MakeUnversionedStringValue(GetColumnValue<TStringBuf>(message, "key")), "key");
        builder.Payload().SetValue(MakeUnversionedStringValue(GetColumnValue<TStringBuf>(message, "data")), "data");
        builder.SetEventTimestamp(::access(GetParameters()->PublicationReleasePath.c_str(), F_OK) == 0
                ? TSystemTimestamp(1000000)
                : TSystemTimestamp(100));
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TPublicationClockReader);

////////////////////////////////////////////////////////////////////////////////

class TReplayReader
    : public TReader
{
public:
    using TReader::DoProcessMessage;
    using TReader::TReader;

    void DoProcessMessage(const TInputMessageConstPtr& input, IOutputCollectorPtr output) override
    {
        // Recreate an already-held deterministic output before the source tries to publish it again.
        auto previousOutput = New<TRootOutputCollector>(
            GetSpec(),
            CreateDeterministicMetaSetter(GetSpec(), EventTimestampAssigner_),
            /*supportsDistribute*/ true);
        TReader::DoProcessMessage(*input, previousOutput->SetParents({input}, {}, {}));
        auto result = previousOutput->CollectResult();
        YT_VERIFY(result.OutputMessages.size() == 1);
        std::vector<TOutputMessageConstPtr> heldOutputs{
            New<TOutputMessage>(std::move(result.OutputMessages.front()), GetContext()->StreamSpecStorage)};
        OutputStore_->TryRegisterKeyedBatch(heldOutputs, *GetContext()->Partition->SourceKey, /*persist*/ false);
        YT_VERIFY(OutputStore_->Contains(*heldOutputs.front()));

        TReader::DoProcessMessage(*input, std::move(output));
    }
};

YT_FLOW_DEFINE_COMPUTATION(TReplayReader);

////////////////////////////////////////////////////////////////////////////////

struct TProcessorParameters
    : public virtual TTransformComputation::TParameters
{
    std::string LineageObservationPath;
    std::string FailComment;
    std::string CommitGateReadyPath;
    std::string CommitGateReleasePath;

    REGISTER_YSON_STRUCT(TProcessorParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("fail_comment", &TThis::FailComment)
            .Default();
        registrar.Parameter("commit_gate_ready_path", &TThis::CommitGateReadyPath)
            .Default();
        registrar.Parameter("commit_gate_release_path", &TThis::CommitGateReleasePath)
            .Default();
        registrar.Parameter("lineage_observation_path", &TThis::LineageObservationPath)
            .Default();
    }
};

class TProcessor
    : public TTransformComputation
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TProcessorParameters);

    using TTransformComputation::TTransformComputation;

    static inline TStreamId OutputStreamId = TStreamId("processed_data");

    void DoInit(IJobInitContextPtr context) override
    {
        TTransformComputation::DoInit(std::move(context));
        if (!GetParameters()->LineageObservationPath.empty()) {
            auto& tracker = GetContext()->JobLineageTracker;
            tracker = New<TRecordingLineageTracker>(tracker, OutputStreamId, TStreamId("data"), GetParameters()->LineageObservationPath);
        }
    }

    void DoSync(IRetryableTransactionPtr transaction) override
    {
        TTransformComputation::DoSync(transaction);
        if (HasInputs_) {
            WaitForCommitGate(GetParameters()->CommitGateReadyPath, GetParameters()->CommitGateReleasePath, GetStatus()->NodeTraverse, GetContext()->Partition->PartitionId);
            if (!GetParameters()->FailComment.empty()) {
                THROW_ERROR_EXCEPTION("Failing before commit with comment %Qv", GetParameters()->FailComment);
            }
        }
        HasInputs_ = false;
    }

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        HasInputs_ = true;
        output->AddMessage(ConvertToOutputMessage(message, OutputStreamId));
    }

private:
    bool HasInputs_ = false;
};

YT_FLOW_DEFINE_COMPUTATION(TProcessor);

////////////////////////////////////////////////////////////////////////////////

class TConsumer
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoProcessMessage(const TMessage& /*message*/, IOutputCollectorPtr /*output*/) override
    {
        NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(1));
    }
};

YT_FLOW_DEFINE_COMPUTATION(TConsumer);

class TSwiftConsumer
    : public TSwiftMapComputation
{
public:
    using TSwiftMapComputation::TSwiftMapComputation;

    void DoProcessMessage(const TMessage& /*message*/, IOutputCollectorPtr /*output*/) override
    {
        NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(1));
    }
};

YT_FLOW_DEFINE_COMPUTATION(TSwiftConsumer);

////////////////////////////////////////////////////////////////////////////////

class TFilteringTestReader
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TMessage& /*message*/, IOutputCollectorPtr output) override
    {
        for (int index = 0; index < 10; ++index) {
            auto builder = MakeOutputMessageBuilder(TStreamId("data"));
            const std::string data(index == 0 ? 8 : 4096, 'x');
            builder.Payload().SetValue(MakeUnversionedStringValue(index == 0 ? "keep" : "drop"), "key");
            builder.Payload().SetValue(MakeUnversionedStringValue(data), "data");
            output->AddMessage(builder.Finish());
        }
    }
};

YT_FLOW_DEFINE_COMPUTATION(TFilteringTestReader);

struct TFilteringSwiftParameters
    : public virtual TSwiftMapComputation::TParameters
{
    std::string LineageObservationPath;
    std::string CommitGateReadyPath;
    std::string CommitGateReleasePath;
    std::string FailComment;

    REGISTER_YSON_STRUCT(TFilteringSwiftParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("lineage_observation_path", &TThis::LineageObservationPath);
        registrar.Parameter("commit_gate_ready_path", &TThis::CommitGateReadyPath).Default();
        registrar.Parameter("commit_gate_release_path", &TThis::CommitGateReleasePath).Default();
        registrar.Parameter("fail_comment", &TThis::FailComment).Default();
    }
};

class TFilteringSwiftProcessor
    : public TSwiftMapComputation
{
public:
    using TSwiftMapComputation::TSwiftMapComputation;

    YT_FLOW_EXTEND_PARAMETERS(TFilteringSwiftParameters);

    void DoInit(IJobInitContextPtr context) override
    {
        TSwiftMapComputation::DoInit(std::move(context));
        auto& tracker = GetContext()->JobLineageTracker;
        tracker = New<TRecordingLineageTracker>(tracker, TStreamId("processed_data"), TStreamId("data"), GetParameters()->LineageObservationPath, [this] {
            // Block after recording completed processing but before entering the shared commit path.
            if (!GetParameters()->CommitGateReadyPath.empty()) {
                WaitForCommitGate(GetParameters()->CommitGateReadyPath, GetParameters()->CommitGateReleasePath, GetStatus()->NodeTraverse, GetContext()->Partition->PartitionId);
            }
            if (!GetParameters()->FailComment.empty()) {
                THROW_ERROR_EXCEPTION("Failing before commit with comment %Qv", GetParameters()->FailComment);
            }
        });
    }

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        output->AddMessage(ConvertToOutputMessage(message, TStreamId("processed_data")));
    }
};

YT_FLOW_DEFINE_COMPUTATION(TFilteringSwiftProcessor);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
