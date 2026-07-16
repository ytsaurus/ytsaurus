#include "pipeline.h"

#include <yt/yt/flow/library/cpp/common/checksum.h>
#include <yt/yt/flow/library/cpp/common/flow_core_version.h>

#include <library/cpp/yt/logging/logger.h>

#include <yt/yt/flow/lib/native_client/public.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/client/api/options.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ypath/helpers.h>

#include <util/system/env.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("FlowClient");

////////////////////////////////////////////////////////////////////////////////

bool IsGracefulUpdateFromEnv()
{
    return FromString<bool>(GetEnv("YT_FLOW_GRACEFUL_UPDATE", "1"));
}

////////////////////////////////////////////////////////////////////////////////

class TLogReader
{
public:
    TLogReader(NApi::IClientPtr client, NYPath::TYPath logTablePath)
        : Client_(std::move(client))
        , LogTablePath_(std::move(logTablePath))
    { }

    bool IsOpen() const
    {
        return CurrentOffset_.has_value();
    }

    void Open()
    {
        CurrentOffset_ = GetTotalRowCount();
    }

    // Reads one batch of log rows. Returns the number of rows actually read.
    i64 Read(TStringBuf dataColumnName = "data")
    {
        YT_VERIFY(IsOpen());

        auto logRows = WaitFor(Client_->PullQueue(
            LogTablePath_,
            /*offset*/ *CurrentOffset_,
            /*partitionIndex*/ 0,
            NQueueClient::TQueueRowBatchReadOptions{}))
            .ValueOrThrow();

        const int valueColumnIndex = logRows->GetNameTable()->GetIdOrThrow(dataColumnName);
        for (const auto& row : logRows->GetRows()) {
            Cerr << row[valueColumnIndex].AsStringBuf() << Endl;
        }

        const i64 rowsRead = logRows->GetFinishOffset() - *CurrentOffset_;
        CurrentOffset_ = logRows->GetFinishOffset();
        return rowsRead;
    }

    // Reads all log rows committed up to the moment of the call.
    void ReadAll(TStringBuf dataColumnName = "data")
    {
        YT_VERIFY(IsOpen());

        const i64 targetOffset = GetTotalRowCount();
        while (*CurrentOffset_ < targetOffset && Read(dataColumnName) > 0) {
            // No-op.
        }
    }

private:
    NApi::IClientPtr Client_;
    NYPath::TYPath LogTablePath_;

    std::optional<i64> CurrentOffset_;

    i64 GetTotalRowCount() const
    {
        auto info = WaitFor(Client_->GetTabletInfos(LogTablePath_, {0}))
            .ValueOrThrow();
        YT_VERIFY(info.size() == 1);
        return info[0].TotalRowCount;
    }
};

////////////////////////////////////////////////////////////////////////////////

void WaitRelativelySmallTime(TInstant activityStart, TDuration minWait, TDuration maxWait)
{
    NConcurrency::TDelayedExecutor::WaitForDuration(
        // Try to overwait not more than 10% of time.
        std::clamp((TInstant::Now() - activityStart) * 0.1, minWait, maxWait));
}

////////////////////////////////////////////////////////////////////////////////

static void WaitPipelineState(
    NApi::IClientPtr client,
    const TYPath& root,
    EPipelineState targetState,
    TDuration waitTimeout,
    TLogReader* logReader = nullptr)
{
    auto deadline = TInstant::Now() + waitTimeout;
    static const int retries = 10;
    EPipelineState currentState = EPipelineState::Unknown;
    THashSet<EPipelineState> targetStates;
    switch (targetState) {
        case EPipelineState::Completed:
            targetStates = {EPipelineState::Completed};
            break;
        case EPipelineState::Working:
            targetStates = {EPipelineState::Completed, EPipelineState::Working};
            break;
        case EPipelineState::Stopped:
            targetStates = {EPipelineState::Stopped};
            break;
        case EPipelineState::Draining:
            targetStates = {EPipelineState::Stopped, EPipelineState::Draining};
            break;
        case EPipelineState::Paused:
            targetStates = {EPipelineState::Paused, EPipelineState::Stopped};
            break;
        case EPipelineState::Pausing:
            targetStates = {EPipelineState::Paused, EPipelineState::Stopped, EPipelineState::Pausing};
            break;
        case EPipelineState::Unknown:
            return;
    }

    const auto startWaitingInstant = TInstant::Now();
    while (true) {
        if (TInstant::Now() > deadline) {
            THROW_ERROR_EXCEPTION("Wait timed out")
                << TErrorAttribute("timeout", waitTimeout);
        }
        int attempt = 0;
        while (true) {
            try {
                currentState = WaitFor(client->GetPipelineState(root)).ValueOrThrow().State;
                break;
            } catch (const std::exception& ex) {
                attempt += 1;
                YT_TLOG_WARNING("Failed to get pipeline state")
                    .With("Attempt", attempt)
                    .With("MaxRetries", retries)
                    .With(ex);
                if (attempt == retries) {
                    throw;
                }
            }
        }
        if (currentState == EPipelineState::Completed && targetState != EPipelineState::Completed) {
            THROW_ERROR_EXCEPTION(
                "Pipeline reached %Qv state while waiting for %Qv; "
                "the controller cannot transition out of Completed. "
                "Recreate the pipeline to recover.",
                currentState,
                targetState);
        }
        if (!targetStates.contains(currentState)) {
            YT_TLOG_INFO("Still waiting")
                .With("CurrentState", currentState)
                .With("TargetState", targetState);
        } else {
            YT_TLOG_INFO("Wait finished")
                .With("CurrentState", currentState)
                .With("TargetState", targetState);
            return;
        }
        if (logReader) {
            logReader->Read();
        }
        WaitRelativelySmallTime(startWaitingInstant, TDuration::MilliSeconds(50), TDuration::Seconds(3));
    }
}

void WaitPipelineState(
    const std::string& clusterUrl,
    const std::optional<std::string>& proxyRole,
    const TYPath& root,
    EPipelineState state,
    TDuration waitTimeout)
{
    auto connection = NApi::NRpcProxy::CreateConnection(
        NApi::NRpcProxy::TConnectionConfig::CreateFromClusterUrl(clusterUrl, proxyRole));
    auto client = connection->CreateClient(NApi::GetClientOptionsFromEnvStatic());

    WaitPipelineState(
        std::move(client),
        root,
        state,
        waitTimeout,
        /*logReader*/ nullptr);
}

void WaitPipelineState(
    NApi::IClientPtr client,
    const TYPath& root,
    EPipelineState state,
    TDuration waitTimeout)
{
    WaitPipelineState(
        std::move(client),
        root,
        state,
        waitTimeout,
        /*logReader*/ nullptr);
}

void RunPipeline(
    const std::string& clusterUrl,
    const std::optional<std::string>& proxyRole,
    const TYPath& root,
    const TPipelineSpecPtr& spec,
    const TDynamicPipelineSpecPtr& dynamicSpec,
    bool setFlowCoreTarget,
    std::optional<bool> graceful,
    TDuration waitTimeout,
    bool enablePipelineCreation,
    bool enablePipelineStopOrPause)
{
    if (!graceful) {
        graceful = IsGracefulUpdateFromEnv();
    }
    ValidatePipelineSpec(spec);
    ValidateDynamicPipelineSpec(dynamicSpec);

    auto connection = NApi::NRpcProxy::CreateConnection(
        NApi::NRpcProxy::TConnectionConfig::CreateFromClusterUrl(clusterUrl, proxyRole));
    auto client = connection->CreateClient(NApi::GetClientOptionsFromEnvStatic());

    TLogReader controllerLogReader(client, NYPath::YPathJoin(root, ControllerLogsTableName));

    const auto startWaitingInstant = TInstant::Now();
    bool fatalError = false;

    while (true) {
        try {
            bool alreadyExists = WaitFor(client->NodeExists(root))
                .ValueOrThrow();

            if (!alreadyExists) {
                if (enablePipelineCreation) {
                    WaitFor(client->CreateNode(root, NObjectClient::EObjectType::Pipeline))
                        .ThrowOnError();
                } else {
                    fatalError = true;

                    THROW_ERROR_EXCEPTION("Pipeline %Qv doesn't exist", root);
                }
            }

            if (!controllerLogReader.IsOpen()) {
                controllerLogReader.Open();
            }

            auto currentState = WaitFor(client->GetPipelineState(root))
                .ValueOrThrow()
                .State;

            THashSet<EPipelineState> targetStates = {
                EPipelineState::Unknown,
                EPipelineState::Paused,
                EPipelineState::Stopped};

            if (enablePipelineStopOrPause) {
                targetStates.insert(EPipelineState::Working);
            }

            if (!targetStates.contains(currentState)) {
                fatalError = true;

                THROW_ERROR_EXCEPTION("Found unexpected pipeline state: %Qv",
                    currentState);
            }

            if (enablePipelineStopOrPause) {
                const auto desiredState = *graceful
                    ? EPipelineState::Stopped
                    : EPipelineState::Paused;

                if (currentState != desiredState && currentState != EPipelineState::Unknown) {
                    if (*graceful) {
                        WaitFor(client->StopPipeline(root))
                            .ThrowOnError();
                        YT_TLOG_INFO("Sent stop");
                    } else {
                        WaitFor(client->PausePipeline(root))
                            .ThrowOnError();
                        YT_TLOG_INFO("Sent pause");
                    }

                    WaitPipelineState(
                        client,
                        root,
                        desiredState,
                        waitTimeout,
                        &controllerLogReader);

                    YT_TLOG_INFO("Stopped");
                }
            }

            if (setFlowCoreTarget) {
                TSetFlowCoreTargetArg arg;
                arg.FlowCoreTarget = TFlowCoreTarget(ResolveFlowCoreVersion());
                arg.AllowUpdateOnPause = true;

                YT_TLOG_INFO("Setting flow core target to Controller")
                    .With("FlowCoreTarget", arg.FlowCoreTarget.Underlying());

                auto resultYson = WaitFor(client->FlowExecute(root, "set-flow-core-target", NYson::ConvertToYsonString(arg)))
                    .ValueOrThrow();

                auto result = NYTree::ConvertTo<TSetFlowCoreTargetResult>(resultYson.Result);

                YT_TLOG_INFO("Updated flow core target")
                    .With("NewVersion", result.Version)
                    .With("FlowCoreTarget", arg.FlowCoreTarget.Underlying());
            }

            {
                TSetPipelineSpecsArg arg;
                arg.Spec = NYTree::ConvertToNode(spec);
                arg.DynamicSpec = NYTree::ConvertToNode(dynamicSpec);
                arg.AllowSpecUpdateOnPause = true;

                // TODO: Enable strict validation later.
                // arg.ValidateStrict = true;

                auto resultYson = WaitFor(client->FlowExecute(root, "set-pipeline-specs", NYson::ConvertToYsonString(arg)))
                    .ValueOrThrow();

                auto result = NYTree::ConvertTo<TSetPipelineSpecsResult>(resultYson.Result);

                YT_TLOG_INFO("Updated pipeline specs")
                    .With("NewSpecVersion", result.SpecVersion)
                    .With("NewDynamicSpecVersion", result.DynamicSpecVersion);
            }

            WaitFor(client->StartPipeline(root))
                .ThrowOnError();
            YT_TLOG_INFO("Sent start");

            WaitPipelineState(
                client,
                root,
                EPipelineState::Working,
                waitTimeout,
                &controllerLogReader);

            break;
        } catch (const std::exception& ex) {
            YT_TLOG_ERROR("Failed to update pipeline")
                .With(ex);

            // Drain everything the controller has produced, so the user sees the public log lines that explain the failure.
            if (controllerLogReader.IsOpen()) {
                try {
                    controllerLogReader.ReadAll();
                } catch (const std::exception& readEx) {
                    YT_TLOG_WARNING("Failed to read controller log")
                        .With(readEx);
                }
            }

            if (fatalError) {
                throw;
            }

            WaitRelativelySmallTime(startWaitingInstant, TDuration::MilliSeconds(250), TDuration::Seconds(5));
        }
    }
}

void WaitPipeline(
    const std::string& clusterUrl,
    const std::optional<std::string>& proxyRole,
    const TYPath& root)
{
    auto pipelinePath = TRichYPath(root);
    pipelinePath.SetCluster(clusterUrl);

    YT_TLOG_INFO("Wait for pipeline to complete")
        .With("Pipeline", pipelinePath);

    auto connection = NApi::NRpcProxy::CreateConnection(
        NApi::NRpcProxy::TConnectionConfig::CreateFromClusterUrl(clusterUrl, proxyRole));
    auto client = connection->CreateClient(NApi::GetClientOptionsFromEnvStatic());

    TLogReader controllerLogReader(client, NYPath::YPathJoin(root, ControllerLogsTableName));

    const auto startWaitingInstant = TInstant::Now();
    while (true) {
        try {
            if (!controllerLogReader.IsOpen()) {
                controllerLogReader.Open();
            }

            while (true) {
                auto status = WaitFor(client->GetPipelineState(root))
                    .ValueOrThrow();

                if (status.State == EPipelineState::Completed) {
                    break;
                }

                YT_TLOG_INFO("Waiting pipeline to complete")
                    .With("CurrentState", status.State)
                    .With("Pipeline", pipelinePath);

                controllerLogReader.Read();

                WaitRelativelySmallTime(startWaitingInstant, TDuration::MilliSeconds(50), TDuration::Seconds(3));
            }

            YT_TLOG_INFO("Pipeline completed")
                .With("Pipeline", pipelinePath);
            break;
        } catch (const std::exception& ex) {
            YT_TLOG_ERROR("Failed to check pipeline")
                .With(ex);
            WaitRelativelySmallTime(startWaitingInstant, TDuration::MilliSeconds(250), TDuration::Seconds(5));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
