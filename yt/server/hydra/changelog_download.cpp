#include "stdafx.h"
#include "changelog_download.h"
#include "config.h"
#include "changelog.h"
#include "changelog_discovery.h"
#include "private.h"

#include <core/concurrency/scheduler.h>

#include <core/misc/serialize.h>

#include <core/logging/log.h>

#include <ytlib/election/cell_manager.h>

#include <ytlib/hydra/hydra_service_proxy.h>

namespace NYT {
namespace NHydra {

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
        LOG_INFO("Requested %v records in changelog %v",
            recordCount,
            changelogId);

        auto changelogOrError = WaitFor(changelogStore->OpenChangelog(changelogId));
        THROW_ERROR_EXCEPTION_IF_FAILED(changelogOrError);
        auto changelog = changelogOrError.Value();
        if (changelog->GetRecordCount() >= recordCount) {
            LOG_INFO("Local changelog already contains %v records, no download needed",
                changelog->GetRecordCount());
            return;
        }

        auto asyncChangelogInfo = DiscoverChangelog(config, cellManager, changelogId, recordCount);
        auto changelogInfo = WaitFor(asyncChangelogInfo);
        if (changelogInfo.ChangelogId == NonexistingSegmentId) {
            THROW_ERROR_EXCEPTION("Unable to find a download source for changelog %v with %v records",
                changelogId,
                recordCount);
        }

        int downloadedRecordCount = changelog->GetRecordCount();

        LOG_INFO("Downloading records %v-%v from peer %v",
            changelog->GetRecordCount(),
            recordCount,
            changelogInfo.PeerId);

        THydraServiceProxy proxy(cellManager->GetPeerChannel(changelogInfo.PeerId));
        proxy.SetDefaultTimeout(config->ChangelogDownloadRpcTimeout);

        while (downloadedRecordCount < recordCount) {
            int desiredChunkSize = std::min(
                config->MaxChangelogRecordsPerRequest,
                recordCount - downloadedRecordCount);

            LOG_DEBUG("Requesting records %v-%v",
                downloadedRecordCount,
                downloadedRecordCount + desiredChunkSize - 1);

            auto req = proxy.ReadChangeLog();
            req->set_changelog_id(changelogId);
            req->set_start_record_id(downloadedRecordCount);
            req->set_record_count(desiredChunkSize);

            auto rsp = WaitFor(req->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error downloading changelog");

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
                LOG_DEBUG("Received records %v-%v while %v records were requested",
                    downloadedRecordCount,
                    downloadedRecordCount + actualChunkSize - 1,
                    desiredChunkSize);
                // Continue anyway.
            } else {
                LOG_DEBUG("Received records %v-%v",
                    downloadedRecordCount,
                    downloadedRecordCount + actualChunkSize - 1);
            }

            for (const auto& data : recordsData) {
                changelog->Append(data);
                ++downloadedRecordCount;
            }
        }

        LOG_INFO("Changelog downloaded successfully");
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error downloading changelog %v", changelogId)
            << ex;
    }
}

} // namespace

TAsyncError DownloadChangelog(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    IChangelogStorePtr changelogStore,
    int changelogId,
    int recordCount)
{
    return BIND(&DoDownloadChangelog)
        .Guarded()
        .AsyncVia(GetHydraIOInvoker())
        .Run(config, cellManager, changelogStore, changelogId, recordCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
