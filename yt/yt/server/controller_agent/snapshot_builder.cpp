#include "snapshot_builder.h"
#include "private.h"
#include "helpers.h"
#include "operation_controller.h"
#include "config.h"
#include "operation.h"

#include <yt/yt/server/lib/controller_agent/serialize.h>

#include <yt/yt/client/api/file_writer.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/core/concurrency/async_stream.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/net/connection.h>

#include <yt/yt/library/process/pipe.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <util/generic/cast.h>

#include <thread>

namespace NYT::NControllerAgent {

using namespace NYTree;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NConcurrency;
using namespace NApi;
using namespace NPipes;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

static const size_t ReadBufferSize = 1_MB;

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
    IClientPtr client,
    IInvokerPtr ioInvoker,
    TIncarnationId incarnationId,
    TForkCountersPtr counters)
    : TForkExecutor(std::move(counters))
    , Config_(config)
    , Client_(client)
    , IOInvoker_(ioInvoker)
    , ControlInvoker_(GetCurrentInvoker())
    , IncarnationId_(incarnationId)
{
    YT_VERIFY(Config_);
    YT_VERIFY(Client_);
    YT_VERIFY(IOInvoker_);

    Logger = ControllerAgentLogger();
}

TFuture<void> TSnapshotBuilder::Run(const TOperationIdToWeakControllerMap& controllers)
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    YT_LOG_INFO("Snapshot builder started");

    std::vector<TOperationId> operationIds;

    YT_LOG_INFO("Preparing controllers for suspension");
    std::vector<TFuture<TSnapshotCookie>> onSnapshotStartedFutures;

    // Capture everything needed in Build.
    for (const auto& [operationId, weakController] : controllers) {
        auto controller = weakController.Lock();
        if (!controller || !controller->IsRunning()) {
            continue;
        }

        auto job = New<TSnapshotJob>();
        job->OperationId = operationId;
        job->WeakController = weakController;
        auto pipe = TPipeFactory().Create();
        job->Reader = CreateZeroCopyAdapter(pipe.CreateAsyncReader(), ReadBufferSize);
        job->OutputFile = std::make_unique<TFile>(FHANDLE(pipe.ReleaseWriteFD()));
        job->Suspended = false;

        Jobs_.push_back(job);

        onSnapshotStartedFutures.push_back(BIND([weakController = job->WeakController] {
                if (auto controller = weakController.Lock()) {
                    return controller->OnSnapshotStarted();
                } else {
                    THROW_ERROR_EXCEPTION("Controller was destroyed before OnSnapshotStarted was called");
                }
            })
            .AsyncVia(controller->GetCancelableInvoker())
            .Run());
        operationIds.push_back(operationId);

        YT_LOG_INFO("Preparing controller for suspension (OperationId: %v)",
            operationId);
    }

    // We need to filter those controllers who were not able to return snapshot cookie
    // on OnSnapshotStarted call. This may normally happen when promise was abandoned
    // because controller was disposed.
    std::vector<TSnapshotJobPtr> preparedJobs;
    YT_PROFILE_TIMING("/controller_agent/snapshot/controllers_prepare_time") {
        auto resultsOrError = WaitFor(AllSet(onSnapshotStartedFutures));
        YT_VERIFY(resultsOrError.IsOK() && "AllSet failed");
        const auto& results = resultsOrError.Value();
        YT_VERIFY(results.size() == Jobs_.size());
        for (int index = 0; index < std::ssize(Jobs_); ++index) {
            const auto& coookieOrError = results[index];
            if (!coookieOrError.IsOK()) {
                YT_LOG_WARNING(coookieOrError, "Failed to get snapshot index from controller (OperationId: %v)",
                    Jobs_[index]->OperationId);
                continue;
            } else if (Jobs_[index]->WeakController.IsExpired()) {
                YT_LOG_INFO("Controller was destroyed between OnSnapshotStarted was called and suspension (OperationId: %v)",
                    Jobs_[index]->OperationId);
            } else {
                Jobs_[index]->Cookie = coookieOrError.Value();
                preparedJobs.emplace_back(Jobs_[index]);
            }
        }
    }

    Jobs_ = std::move(preparedJobs);

    YT_LOG_INFO("Suspending controllers (ControllerCount: %v)", Jobs_.size());

    std::vector<TFuture<void>> operationSuspendFutures;

    for (const auto& job : Jobs_) {
        auto controller = job->WeakController.Lock();
        YT_VERIFY(controller);

        operationSuspendFutures.emplace_back(controller->Suspend()
            .Apply(BIND(&TSnapshotBuilder::OnControllerSuspended, MakeWeak(this), job)
                .AsyncVia(ControlInvoker_)));
    }

    YT_PROFILE_TIMING("/controller_agent/snapshot/controllers_suspend_time") {
        auto result = WaitFor(AllSucceeded(operationSuspendFutures)
            .WithTimeout(Config_->OperationControllerSuspendTimeout));
        if (!result.IsOK()) {
            if (result.GetCode() == NYT::EErrorCode::Timeout) {
                YT_LOG_WARNING("Some of the controllers timed out");
            } else {
                YT_LOG_FATAL(result, "Failed to suspend controllers");
            }
        }
    }

    YT_LOG_INFO("Controllers suspended");

    ControllersSuspended_ = true;

    auto forkFuture = Fork();

    YT_LOG_INFO("Resuming controllers");

    for (const auto& job : Jobs_) {
        if (auto controller = job->WeakController.Lock()) {
            controller->Resume();
        } else {
            // It is a strange situation: how could controller be terminated if its invoker was suspended?
            // The most adequate reaction for us is to not do anything, we no longer have controller anyway.
            YT_LOG_WARNING("Controller was destroyed between suspension and resumption (OperationId: %v)",
                job->OperationId);
        }
    }

    YT_LOG_INFO("Controllers resumed");

    auto uploadFuture = UploadSnapshots()
        .Apply(
            BIND([operationIds, this, this_ = MakeStrong(this)] (const std::vector<TError>& errors) {
                for (size_t i = 0; i < errors.size(); ++i) {
                    const auto& error = errors[i];
                    if (!error.IsOK()) {
                        YT_LOG_INFO(error, "Failed to build snapshot for operation (OperationId: %v)",
                            operationIds[i]);
                    }
                }
            }));
    return AllSucceeded(std::vector<TFuture<void>>{forkFuture, uploadFuture});
}

void TSnapshotBuilder::OnControllerSuspended(const TSnapshotJobPtr& job)
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    if (!ControllersSuspended_) {
        YT_LOG_DEBUG("Controller suspended (OperationId: %v, SnapshotIndex: %v)",
            job->OperationId,
            job->Cookie.SnapshotIndex);
        job->Suspended = true;
    } else {
        YT_LOG_DEBUG("Controller suspended too late (OperationId: %v, SnapshotIndex: %v)",
            job->OperationId,
            job->Cookie.SnapshotIndex);
    }
}

TDuration TSnapshotBuilder::GetTimeout() const
{
    return Config_->SnapshotTimeout;
}

TDuration TSnapshotBuilder::GetForkTimeout() const
{
    return Config_->SnapshotForkTimeout;
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

        try {
            job.Controller->SaveSnapshot(&outputStream);
            outputStream.Finish();
            job.OutputFile->Close();
        } catch (const TFileError& ex) {
            // Failed to save snapshot because other side of the pipe was closed.
        }
    }
}

void TSnapshotBuilder::RunChild()
{
    // TODO(babenko): We have to prevent readers from dying here since once descriptors are closed below
    // the poller will crash. One must come up with a more robust approach.
    static std::vector<NConcurrency::IAsyncZeroCopyInputStreamPtr> readers;
    for (const auto& job : Jobs_) {
        readers.push_back(job->Reader);
    }

    std::vector<int> descriptors = {2};
    for (const auto& job : Jobs_) {
        descriptors.push_back(int(job->OutputFile->GetHandle()));
    }
    CloseAllDescriptors(descriptors);

    std::vector<std::thread> builderThreads;
    {
        const int jobsPerBuilder = Jobs_.size() / Config_->ParallelSnapshotBuilderCount + 1;
        std::vector<TBuildSnapshotJob> jobs;
        for (int jobIndex = 0; jobIndex < std::ssize(Jobs_); ++jobIndex) {
            auto& job = Jobs_[jobIndex];
            auto controller = job->WeakController.Lock();
            if (!job->Suspended || !controller || !controller->IsRunning()) {
                continue;
            }
            TBuildSnapshotJob snapshotJob;
            snapshotJob.OperationId = std::move(job->OperationId);
            // It is OK to pass a strong pointer here since we are in a child process and
            // we do not care about controller lifetime.
            snapshotJob.Controller = std::move(controller);
            snapshotJob.OutputFile = std::move(job->OutputFile);
            jobs.push_back(std::move(snapshotJob));

            if (std::ssize(jobs) >= jobsPerBuilder) {
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
        auto controller = job->WeakController.Lock();
        if (!job->Suspended || !controller || !controller->IsRunning()) {
            job->Reader.Reset();
            continue;
        }
        const auto& cancelableContext = controller->GetCancelableContext();
        auto cancelableInvoker = cancelableContext->CreateInvoker(IOInvoker_);
        auto operationId = job->OperationId;
        auto snapshotIndex = job->Cookie.SnapshotIndex;
        auto uploadFuture = BIND(
            &TSnapshotBuilder::UploadSnapshot,
            MakeStrong(this),
            Passed(std::move(job)))
                .AsyncViaGuarded(
                    cancelableInvoker,
                    TError("Cannot upload snapshot for canceled controller of operation %v", operationId)
                        << TErrorAttribute("operation_id", operationId)
                        << TErrorAttribute("snapshot_index", snapshotIndex))
                .Run();
        snapshotUploadFutures.push_back(std::move(uploadFuture));
    }
    return AllSet(snapshotUploadFutures);
}

void TSnapshotBuilder::UploadSnapshot(const TSnapshotJobPtr& job)
{
    auto operationId = job->OperationId;

    auto Logger = this->Logger
        .WithTag("OperationId: %v", operationId);

    try {
        YT_LOG_INFO("Started uploading snapshot");

        auto snapshotPath = GetSnapshotPath(operationId);
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
            options.PrerequisiteTransactionIds = {IncarnationIdToTransactionId(IncarnationId_)};
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
            attributes->Set("version", ToUnderlying(GetCurrentSnapshotVersion()));
            options.Attributes = std::move(attributes);
            options.Force = true;
            options.Recursive = true;
            auto nodeIdOrError = WaitFor(transaction->CreateNode(
                snapshotUploadPath,
                EObjectType::File,
                options));
            THROW_ERROR_EXCEPTION_IF_FAILED(nodeIdOrError, "Error creating snapshot node");
        }

        i64 snapshotSize = 0;

        // Upload new snapshot.
        {
            TFileWriterOptions options;
            options.Config = Config_->SnapshotWriter;
            auto writer = transaction->CreateFileWriter(snapshotUploadPath, options);

            WaitFor(writer->Open())
                .ThrowOnError();

            while (auto data = WaitFor(job->Reader->Read()).ValueOrThrow()) {
                snapshotSize += data.Size();
                WaitFor(writer->Write(data))
                    .ThrowOnError();
            }

            WaitFor(writer->Close())
                .ThrowOnError();

            YT_LOG_INFO("Snapshot file uploaded successfully (Size: %v, Path: %v)",
                snapshotSize,
                snapshotUploadPath);
        }

        if (snapshotSize == 0) {
            YT_LOG_WARNING("Empty snapshot found, skipping it");
            YT_UNUSED_FUTURE(transaction->Abort());
            return;
        }

        // Commit outer transaction.
        WaitFor(transaction->Commit())
            .ThrowOnError();

        // Atomically move snapshot to the right place.
        {
            TMoveNodeOptions options;
            options.Force = true;
            options.PrerequisiteTransactionIds = {IncarnationIdToTransactionId(IncarnationId_)};

            WaitFor(Client_->MoveNode(
                snapshotUploadPath,
                snapshotPath,
                options))
                .ThrowOnError();

            YT_LOG_INFO("Snapshot file moved successfully (Source: %v, Destination: %v)",
                snapshotUploadPath,
                snapshotPath);
        }

        YT_LOG_INFO("Snapshot uploaded successfully (SnapshotIndex: %v)",
            job->Cookie.SnapshotIndex);

        auto future = VoidFuture;
        if (auto controller = job->WeakController.Lock()) {
            if (controller->IsRunning()) {
                future = BIND(&IOperationController::OnSnapshotCompleted, controller)
                    .AsyncVia(controller->GetCancelableInvoker())
                    .Run(job->Cookie);
            }
        } else {
            YT_LOG_INFO("Controller was destroyed between snapshot upload and OnSnapshotCompleted call");
        }

        // Notify controller about snapshot procedure finish.
        WaitFor(future)
            .ThrowOnError();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Error uploading snapshot");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
