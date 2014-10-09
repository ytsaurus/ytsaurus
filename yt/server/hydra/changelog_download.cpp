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

class TChangelogDownloader
    : public TRefCounted
{
public:
    TChangelogDownloader(
        TDistributedHydraManagerConfigPtr config,
        TCellManagerPtr cellManager,
        IChangelogStorePtr changelogStore)
        : Config(config)
        , CellManager(cellManager)
        , ChangelogStore(changelogStore)
        , Logger(HydraLogger)
    {
        YCHECK(Config);
        YCHECK(CellManager);
        YCHECK(ChangelogStore);

        Logger.AddTag("CellId: %v", CellManager->GetCellId());
    }

    TAsyncError Run(int changelogId, int recordCount)
    {
        return BIND(&TChangelogDownloader::DoRun, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(changelogId, recordCount);
    }

private:
    TDistributedHydraManagerConfigPtr Config;
    TCellManagerPtr CellManager;
    IChangelogStorePtr ChangelogStore;

    NLog::TLogger Logger;


    TError DoRun(int changelogId, int recordCount)
    {
        try {
            LOG_INFO("Requested %v records in changelog %v",
                recordCount,
                changelogId);

            auto changelogOrError = WaitFor(ChangelogStore->OpenChangelog(changelogId));
            THROW_ERROR_EXCEPTION_IF_FAILED(changelogOrError);
            auto changelog = changelogOrError.Value();
            if (changelog->GetRecordCount() >= recordCount) {
                LOG_INFO("Local changelog already contains %v records, no download needed",
                    changelog->GetRecordCount());
                return TError();
            }

            auto asyncChangelogInfo = DiscoverChangelog(Config, CellManager, changelogId, recordCount);
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
            
            THydraServiceProxy proxy(CellManager->GetPeerChannel(changelogInfo.PeerId));
            proxy.SetDefaultTimeout(Config->ChangelogDownloader->RpcTimeout);
            
            while (downloadedRecordCount < recordCount) {
                int desiredChunkSize = std::min(
                    Config->ChangelogDownloader->RecordsPerRequest,
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

            return TError();
        } catch (const std::exception& ex) {
            return TError("Error downloading changelog %v", changelogId)
                << ex;
        }
    }

};

TAsyncError DownloadChangelog(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    IChangelogStorePtr changelogStore,
    int changelogId,
    int recordCount)
{
    auto downloader = New<TChangelogDownloader>(config, cellManager, changelogStore);
    return downloader->Run(changelogId, recordCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
