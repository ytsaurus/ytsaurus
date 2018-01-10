#include "snapshot_builder.h"
#include "private.h"
#include "helpers.h"
#include "operation_controller.h"
#include "serialize.h"

#include <yt/server/scheduler/config.h>
#include <yt/server/scheduler/scheduler.h>

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

////////////////////////////////////////////////////////////////////////////////

struct TBuildSnapshotJob
{
    TOperationPtr Operation;
    std::unique_ptr<TFile> OutputFile;
};

////////////////////////////////////////////////////////////////////////////////

TSnapshotBuilder::TSnapshotBuilder(
    TSchedulerConfigPtr config,
    TSchedulerPtr scheduler,
    IClientPtr client)
    : Config_(config)
    , Scheduler_(scheduler)
    , Client_(client)
    , Profiler(SchedulerProfiler.GetPathPrefix() + "/snapshot")
{
    YCHECK(Config_);
    YCHECK(Scheduler_);
    YCHECK(Client_);

    Logger = SchedulerLogger;
}

TFuture<void> TSnapshotBuilder::Run()
{
    LOG_INFO("Snapshot builder started");

    std::vector<TFuture<void>> operationSuspendFutures;
    std::vector<TOperationId> operationIds;

    LOG_INFO("Suspending controllers");

    // Capture everything needed in Build.
    for (auto operation : Scheduler_->GetOperations()) {
        if (operation->GetState() != EOperationState::Running) {
            continue;
        }

        auto job = New<TSnapshotJob>();
        job->Operation = operation;
        job->Controller = operation->GetController();
        auto pipe = TPipeFactory().Create();
        job->Reader = pipe.CreateAsyncReader();
        job->OutputFile = std::make_unique<TFile>(FHANDLE(pipe.ReleaseWriteFD()));
        job->Suspended = false;
        Jobs_.push_back(job);

        operationSuspendFutures.push_back(job
            ->Controller
            ->Suspend().Apply(
                BIND([=, this_ = MakeStrong(this)] () {
                    if (!ControllersSuspended_) {
                        job->Suspended = true;
                        LOG_DEBUG("Controller suspended (OperationId: %v)",
                            operation->GetId());
                        job->NumberOfJobsToRelease = job->Controller->GetRecentlyCompletedJobCount();
                    } else {
                        LOG_DEBUG("Controller suspended too late (OperationId: %v)",
                            operation->GetId());
                    }
                })
                .AsyncVia(GetCurrentInvoker())
            ));
        operationIds.push_back(operation->GetId());

        LOG_INFO("Snapshot job registered (OperationId: %v)",
            operation->GetId());
    }

    PROFILE_TIMING ("/controllers_suspend_time") {
        auto result = WaitFor(
            Combine(operationSuspendFutures)
                .WithTimeout(Config_->OperationControllerSuspendTimeout));
        if (!result.IsOK()) {
            if (result.GetCode() == NYT::EErrorCode::Timeout) {
                LOG_WARNING("Suspend of some controllers timed out");
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

void DoSnapshotJobs(const std::vector<TBuildSnapshotJob> Jobs_)
{
    for (const auto& job : Jobs_) {
        TUnbufferedFileOutput outputStream(*job.OutputFile);

        auto checkpointableOutput = CreateCheckpointableOutputStream(&outputStream);
        auto bufferedOutput = CreateBufferedCheckpointableOutputStream(checkpointableOutput.get(), PipeWriteBufferSize);

        try {
            job.Operation->GetController()->SaveSnapshot(bufferedOutput.get());
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
            if (!job->Suspended || job->Operation->GetState() != EOperationState::Running) {
                continue;
            }
            TBuildSnapshotJob snapshotJob;
            snapshotJob.Operation = std::move(job->Operation);
            snapshotJob.OutputFile = std::move(job->OutputFile);
            jobs.push_back(std::move(snapshotJob));

            if (jobs.size() >= jobsPerBuilder || jobIndex + 1 == Jobs_.size()) {
                builderThreads.emplace_back(
                    DoSnapshotJobs, std::move(jobs));
                jobs.clear();
            }
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
        if (!job->Suspended || job->Operation->GetState() != EOperationState::Running) {
            continue;
        }
        auto controller = job->Operation->GetController();
        auto cancelableInvoker = controller->GetCancelableContext()->CreateInvoker(
            Scheduler_->GetSnapshotIOInvoker());
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
    const auto& operationId = job->Operation->GetId();

    auto Logger = this->Logger;
    Logger.AddTag("OperationId: %v", operationId);

    try {
        LOG_INFO("Started uploading snapshot");

        auto snapshotPath = GetSnapshotPath(operationId);

        // Start outer transaction.
        ITransactionPtr transaction;
        {
            TTransactionStartOptions options;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set(
                "title",
                Format("Snapshot upload for operation %v", operationId));
            options.Attributes = std::move(attributes);
            auto transactionOrError = WaitFor(
                Client_->StartTransaction(
                    NTransactionClient::ETransactionType::Master,
                    options));
            transaction = transactionOrError.ValueOrThrow();
        }

        // Remove previous snapshot, if exists.
        {
            TRemoveNodeOptions options;
            options.Force = true;
            auto result = WaitFor(transaction->RemoveNode(
                snapshotPath,
                options));
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error removing previous snapshot");
        }

        // Create new snapshot node.
        {
            TCreateNodeOptions options;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("version", GetCurrentSnapshotVersion());
            options.Attributes = std::move(attributes);
            auto result = WaitFor(transaction->CreateNode(
                snapshotPath,
                EObjectType::File,
                options));
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error creating snapshot node");
        }

        // Upload new snapshot.
        {
            TFileWriterOptions options;
            options.Config = Config_->SnapshotWriter;
            auto writer = transaction->CreateFileWriter(snapshotPath, options);

            WaitFor(writer->Open())
                .ThrowOnError();

            auto syncReader = CreateSyncAdapter(job->Reader);
            auto checkpointableInput = CreateCheckpointableInputStream(syncReader.get());

            struct TSnapshotBuilderBufferTag { };
            auto buffer = TSharedMutableRef::Allocate<TSnapshotBuilderBufferTag>(RemoteWriteBufferSize, false);

            while (true) {
                size_t bytesRead = checkpointableInput->Read(buffer.Begin(), buffer.Size());
                if (bytesRead == 0) {
                    break;
                }

                WaitFor(writer->Write(buffer.Slice(0, bytesRead)))
                    .ThrowOnError();
            }

            WaitFor(writer->Close())
                .ThrowOnError();

            LOG_INFO("Snapshot uploaded successfully");
        }

        // Commit outer transaction.
        WaitFor(transaction->Commit())
            .ThrowOnError();

        if (auto controller = job->Operation->GetController()) {
            // Safely remove jobs that we do not need any more.
            WaitFor(controller->ReleaseJobs(job->NumberOfJobsToRelease))
                .ThrowOnError();
        }
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error uploading snapshot");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
