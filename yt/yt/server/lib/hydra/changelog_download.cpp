#include "changelog_download.h"
#include "changelog_discovery.h"
#include "private.h"
#include "hydra_service_proxy.h"
#include "changelog.h"
#include "config.h"

#include <yt/yt/ytlib/election/cell_manager.h>

#include <yt/yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NHydra {

using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

void DoDownloadChangelog(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    IChangelogPtr changelog,
    int recordCount,
    const NLogging::TLogger& logger)
{
    const auto& Logger = logger;

    try {
        YT_TLOG_INFO("Requested changelog records")
            .With("RecordCount", recordCount)
            .With("ChangelogId", changelog->GetId());

        if (changelog->GetRecordCount() >= recordCount) {
            YT_TLOG_INFO("Local changelog already contains enough records, no download needed")
                .With("RecordCount", changelog->GetRecordCount());
            return;
        }

        auto changelogInfoFuture = DiscoverChangelog(config, cellManager, changelog->GetId(), recordCount);
        auto changelogInfo = WaitFor(changelogInfoFuture)
            .ValueOrThrow();
        int downloadedRecordCount = changelog->GetRecordCount();

        YT_TLOG_INFO("Downloading records from peer")
            .With("StartRecord", changelog->GetRecordCount())
            .With("EndRecord", recordCount - 1)
            .With("PeerId", changelogInfo.PeerId);

        TInternalHydraServiceProxy proxy(cellManager->GetPeerChannel(changelogInfo.PeerId));
        proxy.SetDefaultTimeout(config->ChangelogDownloadRpcTimeout);

        while (downloadedRecordCount < recordCount) {
            int desiredChunkSize = std::min(
                config->MaxChangelogRecordsPerRequest,
                recordCount - downloadedRecordCount);

            YT_TLOG_DEBUG("Requesting records")
                .With("StartRecord", downloadedRecordCount)
                .With("EndRecord", downloadedRecordCount + desiredChunkSize - 1);

            auto req = proxy.ReadChangeLog();
            req->set_changelog_id(changelog->GetId());
            req->set_start_record_id(downloadedRecordCount);
            req->set_record_count(desiredChunkSize);

            auto rspOrError = WaitFor(req->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error downloading changelog");
            const auto& rsp = rspOrError.Value();

            const auto& attachments = rsp->Attachments();
            YT_VERIFY(attachments.size() == 1);

            auto records = UnpackRefs(rsp->Attachments()[0]);

            if (records.empty()) {
                THROW_ERROR_EXCEPTION("Peer %v does not have %v records of changelog %v anymore",
                    changelogInfo.PeerId,
                    recordCount,
                    changelog->GetId());
            }

            int actualChunkSize = std::ssize(records);
            YT_TLOG_DEBUG("Received records")
                .With("StartRecord", downloadedRecordCount)
                .With("EndRecord", downloadedRecordCount + actualChunkSize - 1)
                .With("DesiredRecordCount", desiredChunkSize)
                .With("ActualRecordCount", actualChunkSize);

            auto future = changelog->Append(records);
            downloadedRecordCount += std::ssize(records);

            WaitFor(future)
                .ThrowOnError();
        }

        WaitFor(changelog->Flush())
            .ThrowOnError();

        YT_TLOG_INFO("Changelog downloaded successfully");
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error downloading changelog %v", changelog->GetId())
            .With(ex);
    }
}

} // namespace

TFuture<void> DownloadChangelog(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    IChangelogPtr changelog,
    int recordCount,
    const NLogging::TLogger& logger)
{
    return BIND(&DoDownloadChangelog)
        .AsyncVia(GetCurrentInvoker())
        .Run(
            config,
            cellManager,
            changelog,
            recordCount,
            logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
