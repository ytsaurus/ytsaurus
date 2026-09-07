#include <yt/yt/flow/library/cpp/computation/meta_setter.h>
#include <yt/yt/flow/library/cpp/computation/stores/output_store.h>
#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/job_lineage_tracker.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <cstdio>
#include <fstream>

#include <fcntl.h>
#include <unistd.h>

using namespace NYT::NFlow;
using namespace NYT;
using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TRecordingLineageTracker
    : public IJobLineageTracker
{
public:
    TRecordingLineageTracker(IJobLineageTrackerPtr underlying, TStreamId output, TStreamId input, std::string path)
        : Underlying_(std::move(underlying))
        , Output_(std::move(output))
        , Input_(std::move(input))
        , Path_(std::move(path))
    { }

    void Add(TLineageDelta delta) override
    {
        if (const auto* parents = delta.FindPtr(Output_)) {
            if (const auto* value = parents->FindPtr(Input_)) {
                Pending_.Count += value->Count;
                Pending_.ByteSize += value->ByteSize;
                Pending_.InputCount += value->InputCount;
                Pending_.InputByteSize += value->InputByteSize;
            }
        }
        Underlying_->Add(std::move(delta));
    }

    void Commit() override
    {
        Underlying_->Commit();
        if (!Reported_ && (Pending_.Count > 0 || Pending_.InputCount > 0)) {
            const auto temporaryPath = Format("%v.tmp.%v", Path_, TGuid::Create());
            {
                std::ofstream output(temporaryPath);
                output << Pending_.Count << ' ' << Pending_.InputCount << ' '
                       << Pending_.ByteSize << ' ' << Pending_.InputByteSize;
                output.close();
                YT_VERIFY(output.good());
            }
            YT_VERIFY(std::rename(temporaryPath.c_str(), Path_.c_str()) == 0);
            Reported_ = true;
        }
        Pending_ = {};
    }

private:
    const IJobLineageTrackerPtr Underlying_;
    const TStreamId Output_;
    const TStreamId Input_;
    const std::string Path_;
    TLineageDeltaValue Pending_;
    bool Reported_ = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TReaderParameters
    : public virtual TSwiftOrderedSourceComputation::TParameters
{
    std::string LineageCommitPath;
    std::string FailComment;
    std::string CommitGateReadyPath;
    std::string CommitGateReleasePath;

    REGISTER_YSON_STRUCT(TReaderParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("lineage_commit_path", &TThis::LineageCommitPath)
            .Default();
        registrar.Parameter("fail_comment", &TThis::FailComment)
            .Default();
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
        if (!GetParameters()->LineageCommitPath.empty()) {
            auto& tracker = GetContext()->JobLineageTracker;
            tracker = New<TRecordingLineageTracker>(tracker, OutputStreamId, TStreamId("random"), GetParameters()->LineageCommitPath);
        }
    }

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        if (!GetParameters()->FailComment.empty()) {
            THROW_ERROR_EXCEPTION("Failing intentionally with comment %Qv", GetParameters()->FailComment);
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
        if (readyPath.empty() || releasePath.empty()) {
            return;
        }

        if (int fd = ::open(readyPath.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0644); fd >= 0) {
            ::close(fd);
        }
        while (::access(releasePath.c_str(), F_OK) != 0) {
            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
        }
    }
};

YT_FLOW_DEFINE_COMPUTATION(TReader);

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
            /*supportsDistribute*/ true,
            /*collectLineage*/ false);
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
    std::string LineageCommitPath;

    REGISTER_YSON_STRUCT(TProcessorParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("lineage_commit_path", &TThis::LineageCommitPath)
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
        if (!GetParameters()->LineageCommitPath.empty()) {
            auto& tracker = GetContext()->JobLineageTracker;
            tracker = New<TRecordingLineageTracker>(tracker, OutputStreamId, TStreamId("data"), GetParameters()->LineageCommitPath);
        }
    }

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        output->AddMessage(ConvertToOutputMessage(message, OutputStreamId));
    }
};

YT_FLOW_DEFINE_COMPUTATION(TProcessor);

////////////////////////////////////////////////////////////////////////////////

class TConsumer
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoProcessMessage(const TMessage& /*message*/, IOutputCollectorPtr /*output*/) override
    { }
};

YT_FLOW_DEFINE_COMPUTATION(TConsumer);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
