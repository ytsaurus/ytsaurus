#include "stdafx.h"
#include "config.h"
#include "snapshot_builder.h"
#include "scheduler.h"
#include "helpers.h"
#include "private.h"
#include "serialize.h"

#include <core/misc/fs.h>
#include <core/misc/proc.h>

#include <ytlib/scheduler/helpers.h>

#include <ytlib/api/transaction.h>
#include <ytlib/api/file_writer.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NObjectClient;
using namespace NConcurrency;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

static const size_t LocalWriteBufferSize  = (size_t) 1024 * 1024;
static const size_t RemoteWriteBufferSize = (size_t) 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TSnapshotBuilder::TSnapshotBuilder(
    TSchedulerConfigPtr config,
    TSchedulerPtr scheduler,
    IClientPtr client)
    : Config_(config)
    , Scheduler_(scheduler)
    , Client_(client)
{
    YCHECK(Config_);
    YCHECK(Scheduler_);
    YCHECK(Client_);

    Logger = SchedulerLogger;
}

TFuture<void> TSnapshotBuilder::Run()
{
    LOG_INFO("Snapshot builder started");

    try {
        NFS::ForcePath(Config_->SnapshotTempPath);
        NFS::CleanTempFiles(Config_->SnapshotTempPath);
    } catch (const std::exception& ex) {
        return MakeFuture(TError(ex));
    }

    // Capture everything needed in Build.
    for (auto operation : Scheduler_->GetOperations()) {
        if (operation->GetState() != EOperationState::Running)
            continue;

        TJob job;
        job.Operation = operation;
        job.FileName = NFS::CombinePaths(Config_->SnapshotTempPath, ToString(operation->GetId()));
        job.TempFileName = job.FileName + NFS::TempFileSuffix;
        Jobs_.push_back(job);

        LOG_INFO("Snapshot job registered (OperationId: %v)",
            operation->GetId());
    }

    return Fork().Apply(
        BIND(&TSnapshotBuilder::OnBuilt, MakeStrong(this))
            .AsyncVia(Scheduler_->GetSnapshotIOInvoker()));
}

TDuration TSnapshotBuilder::GetTimeout() const
{
    return Config_->SnapshotTimeout;
}

void TSnapshotBuilder::RunChild()
{
    CloseAllDescriptors({
        2 // stderr
    });
    for (const auto& job : Jobs_) {
        Build(job);
    }
}

void TSnapshotBuilder::Build(const TJob& job)
{
    // Save snapshot into a temp file.
    {
        TFileOutput fileOutput(job.TempFileName);
        TBufferedOutput bufferedOutput(&fileOutput, LocalWriteBufferSize);
        auto controller = job.Operation->GetController();
        controller->SaveSnapshot(&bufferedOutput);
    }

    // Move temp file into regular file atomically.
    {
        NFS::Rename(job.TempFileName, job.FileName);
    }
}

void TSnapshotBuilder::OnBuilt()
{
    for (const auto& job : Jobs_) {
        UploadSnapshot(job);
    }

    LOG_INFO("Snapshot builder finished");
}

void TSnapshotBuilder::UploadSnapshot(const TJob& job)
{
    auto operation = job.Operation;

    auto Logger = this->Logger;
    Logger.AddTag("OperationId: %v",
        job.Operation->GetId());

    if (!NFS::Exists(job.FileName)) {
        LOG_WARNING("Snapshot file is missing");
        return;
    }

    if (operation->IsFinishedState()) {
        LOG_INFO("Operation is already finished, snapshot discarded");
        return;
    }

    try {
        LOG_INFO("Started uploading snapshot");

        auto snapshotPath = GetSnapshotPath(operation->GetId());

        // Start outer transaction.
        ITransactionPtr transaction;
        {
            TTransactionStartOptions options;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set(
                "title",
                Format("Snapshot upload for operation %v", operation->GetId()));
            options.Attributes = std::move(attributes);
            auto transactionOrError = WaitFor(
                Client_->StartTransaction(
                NTransactionClient::ETransactionType::Master,
                options));
            THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError);
            transaction = transactionOrError.Value();
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

            {
                auto result = WaitFor(writer->Open());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }

            struct TSnapshotBuilderBufferTag { };
            auto buffer = TSharedMutableRef::Allocate<TSnapshotBuilderBufferTag>(RemoteWriteBufferSize, false);
            TFileInput fileInput(job.FileName);
            TBufferedInput bufferedInput(&fileInput, RemoteWriteBufferSize);

            while (true) {
                size_t bytesRead = bufferedInput.Read(buffer.Begin(), buffer.Size());
                if (bytesRead == 0) {
                    break;
                }

                {
                    auto result = WaitFor(writer->Write(buffer.Slice(0, bytesRead)));
                    THROW_ERROR_EXCEPTION_IF_FAILED(result);
                }
            }

            {
                auto result = WaitFor(writer->Close());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }

            LOG_INFO("Snapshot uploaded successfully");
        }

        // Commit outer transaction.
        {
            auto result = WaitFor(transaction->Commit());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error uploading snapshot");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
