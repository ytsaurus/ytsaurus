#include "stdafx.h"
#include "snapshot_builder.h"
#include "scheduler.h"
#include "helpers.h"
#include "private.h"
#include "serialization_context.h"

#include <core/misc/fs.h>

#include <core/concurrency/fiber.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/file_client/file_writer.h>

#include <ytlib/scheduler/helpers.h>

#include <core/rpc/channel.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <core/ytree/ypath_detail.h>
#include <core/ytree/attribute_helpers.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <server/cell_scheduler/bootstrap.h>

#include <util/stream/file.h>
#include <util/stream/buffered.h>

#include <util/folder/dirut.h>

namespace NYT {
namespace NScheduler {

using namespace NFS;
using namespace NYTree;
using namespace NFileClient;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const size_t LocalWriteBufferSize  = (size_t) 1024 * 1024;
static const size_t RemoteWriteBufferSize = (size_t) 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TSnapshotBuilder::TSnapshotBuilder(
    TSchedulerConfigPtr config,
    NCellScheduler::TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
{
    YCHECK(config);
    YCHECK(bootstrap);

    Logger = SchedulerLogger;
}

TAsyncError TSnapshotBuilder::Run()
{
    LOG_INFO("Snapshot builder started");

    try {
        RemoveDirWithContents(Config->SnapshotTempPath);
        ForcePath(Config->SnapshotTempPath);
    } catch (const std::exception& ex) {
        return MakeFuture(TError(ex));
    }

    // Capture everything needed in Build.
    auto scheduler = Bootstrap->GetScheduler();
    for (auto operation : scheduler->GetOperations()) {
        if (operation->GetState() != EOperationState::Running)
            continue;

        TJob job;
        job.Operation = operation;
        job.FileName = CombinePaths(Config->SnapshotTempPath, ToString(operation->GetOperationId()));
        job.TempFileName = job.FileName + TempFileSuffix;
        Jobs.push_back(job);

        LOG_INFO("Snapshot job registered (OperationId: %s)",
            ~ToString(operation->GetOperationId()));
    }

    return TSnapshotBuilderBase::Run().Apply(
        BIND(&TSnapshotBuilder::OnBuilt, MakeStrong(this))
            .AsyncVia(Bootstrap->GetScheduler()->GetSnapshotIOInvoker()));
}

TDuration TSnapshotBuilder::GetTimeout() const
{
    return Config->SnapshotTimeout;
}

void TSnapshotBuilder::Build()
{
    for (const auto& job : Jobs) {
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
        Rename(job.TempFileName, job.FileName);
    }
}

TError TSnapshotBuilder::OnBuilt(TError error)
{
    if (!error.IsOK()) {
        return error;
    }

    for (const auto& job : Jobs) {
        UploadSnapshot(job);
    }

    LOG_INFO("Snapshot builder finished");

    return TError();
}

void TSnapshotBuilder::UploadSnapshot(const TJob& job)
{
    auto operation = job.Operation;


    NLog::TTaggedLogger Logger(this->Logger);
    Logger.AddTag(Sprintf("OperationId: %s",
        ~ToString(job.Operation->GetOperationId())));

    if (!isexist(~job.FileName)) {
        LOG_WARNING("Snapshot file is missing");
        return;
    }

    if (operation->IsFinishedState()) {
        LOG_INFO("Operation is already finished, snapshot discarded");
        return;
    }

    try {
        LOG_INFO("Started uploading snapshot");

        auto snapshotPath = GetSnapshotPath(operation->GetOperationId());

        auto masterChannel = Bootstrap->GetMasterChannel();
        auto transactionManager = Bootstrap->GetTransactionManager();

        TObjectServiceProxy proxy(masterChannel);

        ITransactionPtr transaction;

        // Start outer transaction.
        {
            TTransactionStartOptions options;
            options.Attributes->Set(
                "title",
                Sprintf("Snapshot upload for operation %s", ~ToString(operation->GetOperationId())));
            transaction = transactionManager->Start(options);
        }

        // Remove previous snapshot, if exists.
        {
            auto req = TYPathProxy::Remove(snapshotPath);
            req->set_force(true);
            SetTransactionId(req, transaction);
            auto rsp = proxy.Execute(req).Get();
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error removing previous snapshot");
        }

        // Create new snapshot node.
        {
            auto req = TCypressYPathProxy::Create(snapshotPath);
            req->set_type(EObjectType::File);
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("version", GetCurrentSnapshotVersion());
            ToProto(req->mutable_node_attributes(), *attributes);
            SetTransactionId(req, transaction);
            auto rsp = proxy.Execute(req).Get();
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error creating snapshot node");
        }

        // Upload new snapshot.
        {
            auto writer = New<TAsyncWriter>(
                Config->SnapshotWriter,
                Bootstrap->GetMasterChannel(),
                transaction,
                transactionManager,
                snapshotPath);

            {
                auto result = WaitFor(writer->AsyncOpen());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }

            TBlob buffer(RemoteWriteBufferSize, false);
            TFileInput fileInput(job.FileName);
            TBufferedInput bufferedInput(&fileInput, RemoteWriteBufferSize);

            while (true) {
                size_t bytesRead = bufferedInput.Read(buffer.Begin(), buffer.Size());
                if (bytesRead == 0) {
                    break;
                }

                {
                    auto result = WaitFor(writer->AsyncWrite(TRef(buffer.Begin(), bytesRead)));
                    THROW_ERROR_EXCEPTION_IF_FAILED(result);
                }
            }

            writer->Close();

            LOG_INFO("Snapshot uploaded successfully");
        }

        // Commit outer transaction.
        {
            auto result = WaitFor(transaction->AsyncCommit());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error uploading snapshot");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
