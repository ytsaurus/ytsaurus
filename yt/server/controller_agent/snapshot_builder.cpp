#include "snapshot_builder.h"
#include "private.h"
#include "helpers.h"
#include "operation_controller.h"
#include "serialize.h"
#include "config.h"
#include "operation.h"

#include <yt/server/scheduler/scheduler.h>
#include <yt/server/scheduler/operation.h>

#include <yt/ytlib/api/file_writer.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/scheduler/helpers.h>

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/misc/checkpointable_stream.h>
#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>

#include <yt/core/pipes/async_reader.h>
#include <yt/core/pipes/async_writer.h>
#include <yt/core/pipes/pipe.h>

#include <yt/core/actions/cancelable_context.h>

#include <thread>

namespace NYT {
namespace NControllerAgent {

using namespace NYTree;
using namespace NObjectClient;
using namespace NConcurrency;
using namespace NApi;
using namespace NPipes;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

static const size_t PipeWriteBufferSize = 1_MB;
static const size_t RemoteWriteBufferSize = 1_MB;

static const TString TmpSuffix = ".tmp";

////////////////////////////////////////////////////////////////////////////////

struct TBuildSnapshotJob
{
    TOperationId OperationId;
    IOperationControllerSnapshotBuilderHostPtr Controller;
    std::unique_ptr<TFile> OutputFile;
};

////////////////////////////////////////////////////////////////////////////////

TSnapshotBuilder::TSnapshotBuilder(
    TControllerAgentConfigPtr config,
    TOperationIdToOperationMap operations,
    IClientPtr client,
    IInvokerPtr ioInvoker)
    : Config_(config)
    , Operations_(std::move(operations))
    , Client_(client)
    , IOInvoker_(ioInvoker)
    , ControlInvoker_(GetCurrentInvoker())
    , Profiler(ControllerAgentProfiler.GetPathPrefix() + "/snapshot")
{
    YCHECK(Config_);
    YCHECK(Client_);
    YCHECK(IOInvoker_);

    Logger = ControllerAgentLogger;
}

TFuture<void> TSnapshotBuilder::Run()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    LOG_INFO("Snapshot builder started");

    std::vector<TOperationId> operationIds;

    LOG_INFO("Preparing controllers for suspension");
    std::vector<TFuture<TSnapshotCookie>> onSnapshotStartedFutures;

    // Capture everything needed.
    for (const auto& pair : Operations_) {
        const auto& operationId = pair.first;
        const auto& operation = pair.second;
        auto controller = operation->GetController();

        if (!controller->IsRunning()) {
            continue;
        }

        auto job = New<TSnapshotJob>();
        job->OperationId = operationId;
        job->Controller = controller;

        auto pipe = TPipeFactory().Create();
        job->Reader = pipe.CreateAsyncReader();
        job->OutputFile = std::make_unique<TFile>(FHANDLE(pipe.ReleaseWriteFD()));
        job->Suspended = false;
        Jobs_.push_back(job);

        // TODO(ignat): migrate here to cancelable invoker (introduce CombineAll that ignores cancellation of combined futures).
        onSnapshotStartedFutures.push_back(BIND(&IOperationControllerSnapshotBuilderHost::OnSnapshotStarted, job->Controller)
            .AsyncVia(job->Controller->GetInvoker())
            .Run());
        operationIds.push_back(operationId);

        LOG_INFO("Preparing controller for suspension (OperationId: %v)",
            operationId);
    }

    // We need to filter those controllers who were not able to return snapshot cookie
    // on OnSnapshotStarted call. This may normally happen when promise was abandoned
    // because controller was disposed.
    std::vector<TSnapshotJobPtr> preparedJobs;
    PROFILE_TIMING("/controllers_prepare_time") {
        auto resultsOrError = WaitFor(CombineAll(onSnapshotStartedFutures));
        YCHECK(resultsOrError.IsOK() && "CombineAll failed");
        const auto& results = resultsOrError.Value();
        YCHECK(results.size() == Jobs_.size());
        for (int index = 0; index < Jobs_.size(); ++index) {
            const auto& coookieOrError = results[index];
            if (!coookieOrError.IsOK()) {
                LOG_WARNING(coookieOrError, "Failed to get snapshot index from controller (OperationId: %v)",
                    Jobs_[index]->OperationId);
                continue;
            }
            Jobs_[index]->Cookie  = coookieOrError.Value();
            preparedJobs.emplace_back(Jobs_[index]);
        }
    }

    Jobs_ = std::move(preparedJobs);

    LOG_INFO("Suspending controllers (ControllerCount: %v)", Jobs_.size());

    std::vector<TFuture<void>> operationSuspendFutures;

    for (const auto& job : Jobs_) {
        operationSuspendFutures.emplace_back(job->Controller->Suspend()
            .Apply(BIND(&TSnapshotBuilder::OnControllerSuspended, MakeWeak(this), job)
                .AsyncVia(ControlInvoker_)));
    }

    PROFILE_TIMING ("/controllers_suspend_time") {
        auto result = WaitFor(Combine(operationSuspendFutures)
            .WithTimeout(Config_->OperationControllerSuspendTimeout));
        if (!result.IsOK()) {
            if (result.GetCode() == NYT::EErrorCode::Timeout) {
                LOG_WARNING("Some of the controllers timed out");
            } else {
                LOG_FATAL(result, "Failed to suspend controllers");
            }
        }
    }

    LOG_INFO("Controllers suspended");

    ControllersSuspended_ = true;

    TFuture<void> forkFuture;
    PROFILE_TIMING ("/fork_time") {
        forkFuture = Fork();
    }

    LOG_INFO("Resuming controllers");

    for (const auto& job : Jobs_) {
        job->Controller->Resume();
    }

    LOG_INFO("Controllers resumed");

    auto uploadFuture = UploadSnapshots()
        .Apply(
            BIND([operationIds, this, this_ = MakeStrong(this)] (const std::vector<TError>& errors) {
                for (size_t i = 0; i < errors.size(); ++i) {
                    const auto& error = errors[i];
                    if (!error.IsOK()) {
                        LOG_INFO(error, "Failed to build snapshot for operation (OperationId: %v)",
                            operationIds[i]);
                    }
                }
            }));
    return Combine(std::vector<TFuture<void>>{forkFuture, uploadFuture});
}

void TSnapshotBuilder::OnControllerSuspended(const TSnapshotJobPtr& job)
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    if (!ControllersSuspended_) {
        LOG_DEBUG("Controller suspended (OperationId: %v, SnapshotIndex: %v)",
            job->OperationId,
            job->Cookie.SnapshotIndex);
        job->Suspended = true;
    } else {
        LOG_DEBUG("Controller suspended too late (OperationId: %v, SnapshotIndex: %v)",
            job->OperationId,
            job->Cookie.SnapshotIndex);
    }
}

TDuration TSnapshotBuilder::GetTimeout() const
{
    return Config_->SnapshotTimeout;
}

void TSnapshotBuilder::RunParent()
{
    for (const auto& job : Jobs_) {
        job->OutputFile->Close();
    }
}

void DoSnapshotJobs(const std::vector<TBuildSnapshotJob> jobs)
{
    for (const auto& job : jobs) {
        TFileOutput outputStream(*job.OutputFile);

        auto checkpointableOutput = CreateCheckpointableOutputStream(&outputStream);
        auto bufferedOutput = CreateBufferedCheckpointableOutputStream(checkpointableOutput.get(), PipeWriteBufferSize);

        try {
            job.Controller->SaveSnapshot(bufferedOutput.get());
            bufferedOutput->Finish();
            job.OutputFile->Close();
        } catch (const TFileError& ex) {
            // Failed to save snapshot because other side of the pipe was closed.
        }
    }
}

void TSnapshotBuilder::RunChild()
{
    std::vector<int> descriptors = {2};
    for (const auto& job : Jobs_) {
        descriptors.push_back(int(job->OutputFile->GetHandle()));
    }
    CloseAllDescriptors(descriptors);

    std::vector<std::thread> builderThreads;
    {
        const int jobsPerBuilder = Jobs_.size() / Config_->ParallelSnapshotBuilderCount + 1;
        std::vector<TBuildSnapshotJob> jobs;
        for (int jobIndex = 0; jobIndex < Jobs_.size(); ++jobIndex) {
            auto& job = Jobs_[jobIndex];
            if (!job->Suspended || !job->Controller->IsRunning()) {
                continue;
            }
            TBuildSnapshotJob snapshotJob;
            snapshotJob.OperationId = std::move(job->OperationId);
            snapshotJob.Controller = std::move(job->Controller);
            snapshotJob.OutputFile = std::move(job->OutputFile);
            jobs.push_back(std::move(snapshotJob));

            if (jobs.size() >= jobsPerBuilder) {
                builderThreads.emplace_back(
                    DoSnapshotJobs, std::move(jobs));
                jobs.clear();
            }
        }

        if (jobs.size() > 0) {
            builderThreads.emplace_back(
                DoSnapshotJobs, std::move(jobs));
        }
        Jobs_.clear();
    }

    for (auto& builderThread : builderThreads) {
        builderThread.join();
    }
}

TFuture<std::vector<TError>> TSnapshotBuilder::UploadSnapshots()
{
    std::vector<TFuture<void>> snapshotUploadFutures;
    for (auto& job : Jobs_) {
        if (!job->Suspended || !job->Controller->IsRunning()) {
            continue;
        }
        auto cancelableInvoker = job->Controller->GetCancelableContext()->CreateInvoker(IOInvoker_);
        auto uploadFuture = BIND(
            &TSnapshotBuilder::UploadSnapshot,
            MakeStrong(this),
            Passed(std::move(job)))
                .AsyncVia(cancelableInvoker)
                .Run();
        snapshotUploadFutures.push_back(std::move(uploadFuture));
    }
    return CombineAll(snapshotUploadFutures);
}

void TSnapshotBuilder::UploadSnapshot(const TSnapshotJobPtr& job)
{
    const auto& operationId = job->OperationId;

    auto Logger = this->Logger;
    Logger.AddTag("OperationId: %v", operationId);

    try {
        LOG_INFO("Started uploading snapshot");

        auto snapshotPath = GetNewSnapshotPath(operationId);
        auto snapshotUploadPath = snapshotPath + TmpSuffix;

        // Start outer transaction.
        ITransactionPtr transaction;
        {
            TTransactionStartOptions options;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set(
                "title",
                Format("Snapshot upload for operation %v", operationId));
            options.Attributes = std::move(attributes);
            options.Timeout = Config_->SnapshotTimeout;
            auto transactionOrError = WaitFor(
                Client_->StartTransaction(
                    NTransactionClient::ETransactionType::Master,
                    options));
            transaction = transactionOrError.ValueOrThrow();
        }

        // Create new snapshot node.
        {
            TCreateNodeOptions options;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("version", GetCurrentSnapshotVersion());
            options.Attributes = std::move(attributes);
            options.Force = true;
            options.Recursive = true;
            auto result = WaitFor(transaction->CreateNode(
                snapshotUploadPath,
                EObjectType::File,
                options));
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error creating snapshot node");
        }

        i64 snapshotSize = 0;

        // Upload new snapshot.
        {
            TFileWriterOptions options;
            options.Config = Config_->SnapshotWriter;
            auto writer = transaction->CreateFileWriter(snapshotUploadPath, options);

            WaitFor(writer->Open())
                .ThrowOnError();

            auto syncReader = CreateSyncAdapter(job->Reader);
            auto checkpointableInput = CreateCheckpointableInputStream(syncReader.get());

            struct TSnapshotBuilderBufferTag { };
            auto buffer = TSharedMutableRef::Allocate<TSnapshotBuilderBufferTag>(RemoteWriteBufferSize, false);

            while (true) {
                size_t bytesRead = checkpointableInput->Read(buffer.Begin(), buffer.Size());
                snapshotSize += bytesRead;
                if (bytesRead == 0) {
                    break;
                }

                WaitFor(writer->Write(buffer.Slice(0, bytesRead)))
                    .ThrowOnError();
            }

            WaitFor(writer->Close())
                .ThrowOnError();

            LOG_INFO("Snapshot file uploaded successfully (Size: %v, Path: %v)",
                snapshotSize,
                snapshotUploadPath);
        }

        if (snapshotSize == 0) {
            LOG_WARNING("Empty snapshot found, skipping it");
            transaction->Abort();
            return;
        }

        // Commit outer transaction.
        WaitFor(transaction->Commit())
            .ThrowOnError();

        // Atomically move snapshot to the right place.
        {
            TMoveNodeOptions options;
            options.Force = true;

            WaitFor(Client_->MoveNode(
                snapshotUploadPath,
                snapshotPath,
                options))
                .ThrowOnError();

            LOG_INFO("Snapshot file moved successfully (Source: %v, Destination: %v)",
                snapshotUploadPath,
                snapshotPath);
        }

        // Copy snapshot to old operation node if such node exists.
        {
            TCopyNodeOptions options;
            options.Recursive = false;
            options.Force = true;

            auto snapshotOldPath = GetSnapshotPath(operationId);

            auto error = WaitFor(Client_->CopyNode(
                snapshotPath,
                snapshotOldPath,
                options));

            if (!error.IsOK()) {
                // COMPAT: Remove message check when masters are updated and will set ResolveError
                // if intermediate node is missing.
                auto isIntermediateNodeMissing = error.FindMatching(NYTree::EErrorCode::ResolveError) ||
                    error.GetMessage().Contains("has no child");

                // Intermediate nodes can be missing in new operations storage mode.
                if (!isIntermediateNodeMissing) {
                    THROW_ERROR error;
                }
            } else {
                LOG_INFO("Snapshot file copied successfully (Source: %v, Destination: %v)",
                    snapshotPath,
                    snapshotOldPath);
            }
        }

        LOG_INFO("Snapshot uploaded successfully (SnapshotIndex: %v)",
            job->Cookie.SnapshotIndex);

        auto controller = job->Controller;
        if (controller->IsRunning()) {
            // Safely remove jobs that we do not need any more.
            WaitFor(
                BIND(&IOperationController::OnSnapshotCompleted, controller)
                    .AsyncVia(controller->GetCancelableInvoker())
                    .Run(job->Cookie))
                .ThrowOnError();
        }

    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error uploading snapshot");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
