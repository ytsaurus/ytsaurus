#include "local_hydra_janitor.h"
#include "config.h"
#include "file_helpers.h"
#include "private.h"
#include "hydra_janitor_helpers.h"

#include <yt/core/misc/fs.h>

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT::NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HydraLogger;

////////////////////////////////////////////////////////////////////////////////

class TLocalHydraJanitor::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TString snapshotPath,
        TString changelogPath,
        TLocalHydraJanitorConfigPtr config,
        IInvokerPtr invoker)
        : SnapshotPath_(std::move(snapshotPath))
        , ChangelogPath_(std::move(changelogPath))
        , Config_(std::move(config))
        , PeriodicExecutor_(New<TPeriodicExecutor>(
            std::move(invoker),
            BIND(&TImpl::OnCleanup, MakeWeak(this)),
            Config_->CleanupPeriod))
    { }

    void Start()
    {
        PeriodicExecutor_->Start();
    }

private:
    const TString SnapshotPath_;
    const TString ChangelogPath_;
    const TLocalHydraJanitorConfigPtr Config_;

    const TPeriodicExecutorPtr PeriodicExecutor_;


    std::vector<THydraFileInfo> ListFiles(const TString& path, const TString& extension)
    {
        std::vector<THydraFileInfo> result;
        auto fileNames = NFS::EnumerateFiles(path);
        for (const auto& fileName : fileNames) {
            if (NFS::GetFileExtension(fileName) != extension) {
                continue;
            }

            int id;
            i64 size;
            try {
                id = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
                size = NFS::GetFileStatistics(NFS::CombinePaths(path, fileName)).Size;
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Janitor has found a broken persistence file (FileName: %v)",
                    fileName,
                    path);
                continue;
            }
            result.push_back({id, size});
        }
        return result;
    }

    void RemoveFiles(const TString& path, const TString& extension, int thresholdId)
    {
        std::vector<THydraFileInfo> result;
        auto fileNames = NFS::EnumerateFiles(path);
        for (const auto& fileName : fileNames) {
            if (NFS::GetFileExtension(fileName) != extension) {
                continue;
            }

            int id;
            try {
                id = FromString<int>(NFS::GetFileNameWithoutExtension(fileName));
            } catch (const std::exception&) {
                // Ignore, cf. logging above.
                continue;
            }

            if (id >= thresholdId) {
                continue;
            }

            YT_LOG_INFO("Janitor is removing persistence file (FileName: %v)", fileName);
            try {
                NFS::Remove(NFS::CombinePaths(path, fileName));
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Janitor is unable to remove persistence file (FileName: %v)",
                    fileName);
            }
        }
    }

    void OnCleanup()
    {
        auto snapshots = ListFiles(SnapshotPath_, SnapshotExtension);
        auto changelogs = ListFiles(SnapshotPath_, ChangelogExtension);

        auto thresholdId = ComputeJanitorThresholdId(
            snapshots,
            changelogs,
            Config_);

        RemoveFiles(SnapshotPath_, SnapshotExtension, thresholdId);
        RemoveFiles(ChangelogPath_, ChangelogExtension, thresholdId);
    }
};

////////////////////////////////////////////////////////////////////////////////

TLocalHydraJanitor::TLocalHydraJanitor(
    TString snapshotPath,
    TString changelogPath,
    TLocalHydraJanitorConfigPtr config,
    IInvokerPtr invoker)
    : Impl_(New<TImpl>(
        std::move(snapshotPath),
        std::move(changelogPath),
        std::move(config),
        std::move(invoker)))
{ }

void TLocalHydraJanitor::Start()
{
    Impl_->Start();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
