#include "config.h"
#include "file_helpers.h"
#include "local_snapshot_janitor.h"
#include "private.h"
#include "snapshot_quota_helpers.h"

#include <yt/core/misc/fs.h>

#include <yt/core/actions/public.h>

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT::NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HydraLogger;

////////////////////////////////////////////////////////////////////////////////

class TLocalSnapshotJanitor
    : public ILocalSnapshotJanitor
{
public:
    explicit TLocalSnapshotJanitor(
        TLocalSnapshotJanitorConfigPtr config,
        IInvokerPtr invoker)
        : Config_(std::move(config))
        , SnapshotCleanupExecutor_(New<TPeriodicExecutor>(
            std::move(invoker),
            BIND(&TLocalSnapshotJanitor::OnSnapshotCleanup, MakeWeak(this)),
            Config_->CleanupPeriod))
    { }

    virtual void Start() override
    {
        SnapshotCleanupExecutor_->Start();
    }

private:
    const TLocalSnapshotJanitorConfigPtr Config_;

    TPeriodicExecutorPtr SnapshotCleanupExecutor_;

    void OnSnapshotCleanup()
    {
        const auto& snapshotsPath = Config_->Snapshots->Path;
        std::vector<TSnapshotInfo> snapshots;
        auto snapshotFileNames = NFS::EnumerateFiles(snapshotsPath);
        for (const auto& fileName : snapshotFileNames) {
            if (NFS::GetFileExtension(fileName) != SnapshotExtension)
                continue;

            int snapshotId;
            i64 snapshotSize;
            try {
                snapshotId = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
                snapshotSize = NFS::GetFileStatistics(NFS::CombinePaths(snapshotsPath, fileName)).Size;
            } catch (const std::exception& ex) {
                YT_LOG_WARNING("Unrecognized item %v in snapshot store",
                    fileName);
                continue;
            }
            snapshots.push_back({snapshotId, snapshotSize});
        }

        auto thresholdId = NHydra::GetSnapshotThresholdId(
            snapshots,
            Config_->MaxSnapshotCountToKeep,
            Config_->MaxSnapshotSizeToKeep);

        for (const auto& fileName : snapshotFileNames) {
            if (NFS::GetFileExtension(fileName) != SnapshotExtension)
                continue;

            int snapshotId;
            try {
                snapshotId = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
            } catch (const std::exception& ex) {
                // Ignore, cf. logging above.
                continue;
            }

            if (snapshotId < thresholdId) {
                YT_LOG_INFO("Removing snapshot %v",
                    snapshotId);

                try {
                    NFS::Remove(NFS::CombinePaths(snapshotsPath, fileName));
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex, "Error removing %v from snapshot store",
                        fileName);
                }
            }
        }

        const auto& changelogsPath = Config_->Changelogs->Path;
        auto changelogFileNames = NFS::EnumerateFiles(changelogsPath);
        for (const auto& fileName : changelogFileNames) {
            if (NFS::GetFileExtension(fileName) != ChangelogExtension)
                continue;

            int changelogId;
            try {
                changelogId = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
            } catch (const std::exception& ex) {
                YT_LOG_WARNING("Unrecognized item %v in changelog store",
                    fileName);
                continue;
            }

            if (changelogId < thresholdId) {
                YT_LOG_INFO("Removing changelog %v",
                    changelogId);
                try {
                    RemoveChangelogFiles(NFS::CombinePaths(changelogsPath, fileName));
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex, "Error removing %v from changelog store",
                        fileName);
                }
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ILocalSnapshotJanitorPtr CreateLocalSnapshotJanitor(
    TLocalSnapshotJanitorConfigPtr config,
    IInvokerPtr invoker)
{
    return New<TLocalSnapshotJanitor>(
        std::move(config),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
