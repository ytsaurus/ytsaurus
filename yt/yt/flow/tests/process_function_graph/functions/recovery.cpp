#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/connectors/common/sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>

#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <util/string/hex.h>

#include <cerrno>
#include <fcntl.h>
#include <fstream>
#include <unistd.h>

namespace NYT::NFlow::NProcessFunctionGraphTest {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool Exists(const std::string& path)
{
    return ::access(path.c_str(), F_OK) == 0;
}

bool CreateMarker(const std::string& path, const std::string& value)
{
    int fd = ::open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0644);
    if (fd < 0) {
        THROW_ERROR_EXCEPTION_UNLESS(errno == EEXIST, "Cannot create recovery test marker %Qv", path);
        return false;
    }
    auto written = ::write(fd, value.data(), value.size());
    ::close(fd);
    THROW_ERROR_EXCEPTION_UNLESS(written == std::ssize(value), "Cannot write recovery test marker %Qv", path);
    return true;
}

void AppendLine(const std::string& path, const std::string& value)
{
    std::ofstream output(path, std::ios::app);
    output << value << '\n';
    output.flush();
    THROW_ERROR_EXCEPTION_UNLESS(output.good(), "Cannot write recovery test log %Qv", path);
}

void WaitForMarker(const std::string& path)
{
    while (!Exists(path)) {
        NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(20));
    }
}

std::string EncodeId(const TMessageId& id)
{
    return std::string(HexEncode(id.Underlying()));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

struct TGraphRecoveryParameters
    : public TYsonStruct
{
    std::string MarkerPath;
    bool ThrowOnce = false;

    REGISTER_YSON_STRUCT(TGraphRecoveryParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("marker_path", &TThis::MarkerPath)
            .NonEmpty();
        registrar.Parameter("throw_once", &TThis::ThrowOnce)
            .Default(false);
    }
};

//! Stops the last graph node after upstream state, messages and timers have been produced.
class TGraphRecoveryFunction
    : public IBatchProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& context) override
    {
        Parameters_ = context->GetParameters<TGraphRecoveryParameters>();
        PartitionId_ = ToString(context->GetPartitionId());
    }

    void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& context) override
    {
        if (input->GetMessages().empty()) {
            return;
        }
        AppendLine(Parameters_->MarkerPath + ".attempts", ToString(context->GetEpochUniqueSeqNo()));
        if (CreateMarker(Parameters_->MarkerPath, PartitionId_ + "\n") && Parameters_->ThrowOnce) {
            WaitForMarker(Parameters_->MarkerPath + ".throw");
            THROW_ERROR_EXCEPTION("Injected process function graph failure before commit");
        }
        WaitForMarker(Parameters_->MarkerPath + ".release");
    }

private:
    TIntrusivePtr<TGraphRecoveryParameters> Parameters_;
    std::string PartitionId_;
};

struct TGraphDeliveryParameters
    : public TYsonStruct
{
    std::string IdsPath;

    REGISTER_YSON_STRUCT(TGraphDeliveryParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("ids_path", &TThis::IdsPath)
            .NonEmpty();
    }
};

//! Records the materialized graph IDs seen by a separate downstream computation.
class TGraphDeliveryFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& context) override
    {
        IdsPath_ = context->GetParameters<TGraphDeliveryParameters>()->IdsPath;
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        AppendLine(IdsPath_, EncodeId(message->MessageId));
        output->AddMessage(context->ConvertToOutputMessage(*message));
    }

private:
    std::string IdsPath_;
};

struct TGraphCommitFaultParameters
    : public ISink::TParameters
{
    std::string MarkerPath;
    bool ExitAfterCommit = false;

    REGISTER_YSON_STRUCT(TGraphCommitFaultParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("marker_path", &TThis::MarkerPath)
            .NonEmpty();
        registrar.Parameter("exit_after_commit", &TThis::ExitAfterCommit)
            .Default(false);
    }
};

class TGraphCommitFaultSinkController
    : public TSinkControllerBase
{
public:
    using TSinkControllerBase::TSinkControllerBase;

    std::optional<i64> GetReceiverChannelCount() override
    {
        return std::nullopt;
    }
};

//! Injects failures after a successful epoch transaction, before host post-commit actions.
class TGraphCommitFaultSink
    : public TSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TGraphCommitFaultParameters);
    using TSinkController = TGraphCommitFaultSinkController;

    using TSinkBase::TSinkBase;

    void Init(IInitContextPtr /*initContext*/) override
    { }

    void Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed) override
    {
        MessageIds_.push_back(message->MessageId);
        onDistributed();
    }

    void Sync(NApi::IDynamicTableTransactionPtr transaction) override
    {
        auto ids = std::exchange(MessageIds_, {});
        const auto& path = GetParameters()->MarkerPath;
        if (ids.empty() || Exists(path)) {
            return;
        }
        auto* retryable = dynamic_cast<IRetryableTransaction*>(transaction.Get());
        YT_VERIFY(retryable);
        retryable->Apply(BIND([path] (const NApi::ITransactionPtr& /*transaction*/) {
            // A lost successful response must be resolved through the durable marker, without another write attempt.
            if (Exists(path)) {
                CreateMarker(path + ".replayed", "unexpected write attempt\n");
            }
        }));
        retryable->SubscribeOnAttemptResult(BIND([
            path,
            exitAfterCommit = GetParameters()->ExitAfterCommit,
            ids = std::move(ids)] (const TCommitAttemptResult& result) {
            if (!result.CommitResult.IsOK()) {
                return;
            }
            std::string contents;
            for (const auto& id : ids) {
                contents += EncodeId(id) + "\n";
            }
            if (!CreateMarker(path, contents)) {
                return;
            }
            if (exitAfterCommit) {
                ::_exit(0);
            }
            THROW_ERROR_EXCEPTION(NYT::EErrorCode::Timeout, "Injected lost successful commit response");
        }));
    }

    void Commit() override
    { }

private:
    std::vector<TMessageId> MessageIds_;
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphRecoveryFunction, TGraphRecoveryParameters);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TGraphDeliveryFunction, TGraphDeliveryParameters);
YT_FLOW_DEFINE_SINK(TGraphCommitFaultSink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NProcessFunctionGraphTest
