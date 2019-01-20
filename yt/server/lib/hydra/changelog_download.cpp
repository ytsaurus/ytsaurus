#include "changelog_download.h"
#include "private.h"
#include "changelog.h"
#include "changelog_discovery.h"
#include "config.h"

#include <yt/ytlib/election/cell_manager.h>

#include <yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/core/concurrency/scheduler.h>

namespace NYT::NHydra {

using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HydraLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

void DoDownloadChangelog(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    IChangelogStorePtr changelogStore,
    int changelogId,
    int recordCount)
{
    try {
        YT_LOG_INFO("Requested %v records in changelog %v",
            recordCount,
            changelogId);

        auto asyncChangelog = changelogStore->OpenChangelog(changelogId);
        auto changelog = WaitFor(asyncChangelog)
            .ValueOrThrow();

        if (changelog->GetRecordCount() >= recordCount) {
            YT_LOG_INFO("Local changelog already contains %v records, no download needed",
                changelog->GetRecordCount());
            return;
        }

        auto asyncChangelogInfo = DiscoverChangelog(config, cellManager, changelogId, recordCount);
        auto changelogInfo = WaitFor(asyncChangelogInfo)
            .ValueOrThrow();
        int downloadedRecordCount = changelog->GetRecordCount();

        YT_LOG_INFO("Downloading records %v-%v from peer %v",
            changelog->GetRecordCount(),
            recordCount - 1,
            changelogInfo.PeerId);

        THydraServiceProxy proxy(cellManager->GetPeerChannel(changelogInfo.PeerId));
        proxy.SetDefaultTimeout(config->ChangelogDownloadRpcTimeout);

        while (downloadedRecordCount < recordCount) {
            int desiredChunkSize = std::min(
                config->MaxChangelogRecordsPerRequest,
                recordCount - downloadedRecordCount);

            YT_LOG_DEBUG("Requesting records %v-%v",
                downloadedRecordCount,
                downloadedRecordCount + desiredChunkSize - 1);

            auto req = proxy.ReadChangeLog();
            req->set_changelog_id(changelogId);
            req->set_start_record_id(downloadedRecordCount);
            req->set_record_count(desiredChunkSize);

            auto rspOrError = WaitFor(req->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error downloading changelog");
            const auto& rsp = rspOrError.Value();

            const auto& attachments = rsp->Attachments();
            YCHECK(attachments.size() == 1);

            std::vector<TSharedRef> recordsData;
            UnpackRefs(rsp->Attachments()[0], &recordsData);

            if (recordsData.empty()) {
                THROW_ERROR_EXCEPTION("Peer %v does not have %v records of changelog %v anymore",
                    changelogInfo.PeerId,
                    recordCount,
                    changelogId);
            }

            int actualChunkSize = static_cast<int>(recordsData.size());
            if (actualChunkSize != desiredChunkSize) {
                YT_LOG_DEBUG("Received records %v-%v while %v records were requested",
                    downloadedRecordCount,
                    downloadedRecordCount + actualChunkSize - 1,
                    desiredChunkSize);
                // Continue anyway.
            } else {
                YT_LOG_DEBUG("Received records %v-%v",
                    downloadedRecordCount,
                    downloadedRecordCount + actualChunkSize - 1);
            }

            TFuture<void> asyncResult;
            for (const auto& data : recordsData) {
                asyncResult = changelog->Append(data);
                ++downloadedRecordCount;
            }

            if (asyncResult) {
                WaitFor(asyncResult)
                    .ThrowOnError();
            }
        }

        WaitFor(changelog->Flush())
            .ThrowOnError();

        YT_LOG_INFO("Changelog downloaded successfully");
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error downloading changelog %v", changelogId)
            << ex;
    }
}

} // namespace

TFuture<void> DownloadChangelog(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    IChangelogStorePtr changelogStore,
    int changelogId,
    int recordCount)
{
    return BIND(&DoDownloadChangelog)
        .AsyncVia(GetCurrentInvoker())
        .Run(config, cellManager, changelogStore, changelogId, recordCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
