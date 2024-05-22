#include "private.h"
#include "local_hydra_janitor.h"
#include "config.h"
#include "file_helpers.h"
#include "hydra_janitor_helpers.h"

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = HydraLogger;

////////////////////////////////////////////////////////////////////////////////

class TLocalHydraJanitor
    : public ILocalHydraJanitor
{
public:
    TLocalHydraJanitor(
        TString snapshotPath,
        TString changelogPath,
        TLocalHydraJanitorConfigPtr config,
        IInvokerPtr ioInvoker)
        : SnapshotPath_(std::move(snapshotPath))
        , ChangelogPath_(std::move(changelogPath))
        , PeriodicExecutor_(New<TPeriodicExecutor>(
            std::move(ioInvoker),
            BIND(&TLocalHydraJanitor::OnCleanup, MakeWeak(this))))
        , Config_(std::move(config))
    { }

    void Initialize() override
    {
        PeriodicExecutor_->SetPeriod(GetConfig()->CleanupPeriod);

        if (GetConfig()->EnableLocalJanitor) {
            PeriodicExecutor_->Start();
        }
    }

    void Reconfigure(const TDynamicLocalHydraJanitorConfigPtr& dynamicConfig) override
    {
        auto oldConfig = GetConfig();
        auto newConfig = oldConfig->ApplyDynamic(dynamicConfig);

        PeriodicExecutor_->SetPeriod(newConfig->CleanupPeriod);

        if (newConfig->EnableLocalJanitor != oldConfig->EnableLocalJanitor) {
            if (newConfig->EnableLocalJanitor) {
                PeriodicExecutor_->Start();
            } else {
                YT_UNUSED_FUTURE(PeriodicExecutor_->Stop());
            }

            YT_LOG_DEBUG("Janitor was %v",
                newConfig->EnableLocalJanitor ? "enabled" : "disabled");
        }

        Config_.Store(std::move(newConfig));
    }

    TLocalHydraJanitorConfigPtr GetConfig() const
    {
        return Config_.Acquire();
    }

private:
    const TString SnapshotPath_;
    const TString ChangelogPath_;

    const TPeriodicExecutorPtr PeriodicExecutor_;

    TAtomicIntrusivePtr<TLocalHydraJanitorConfig> Config_;

    static int ParseFileId(const TString& fileName, const TString& suffix)
    {
        return FromString<int>(fileName.substr(0, fileName.length() - suffix.length()));
    }

    std::vector<THydraFileInfo> ListFiles(const TString& path, const TString& suffix)
    {
        std::vector<THydraFileInfo> result;
        auto fileNames = NFS::EnumerateFiles(path);
        for (const auto& fileName : fileNames) {
            if (!fileName.EndsWith(suffix)) {
                continue;
            }

            auto fullFileName = NFS::CombinePaths(path, fileName);

            int id;
            i64 size;
            try {
                id = ParseFileId(fileName, suffix);
                size = NFS::GetPathStatistics(fullFileName).Size;
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Janitor has found a broken Hydra file (FileName: %v)",
                    fullFileName);
                continue;
            }
            result.push_back({id, size});
        }
        return result;
    }

    void RemoveFiles(const TString& path, const TString& suffix, int thresholdId)
    {
        std::vector<THydraFileInfo> result;
        auto fileNames = NFS::EnumerateFiles(path);
        for (const auto& fileName : fileNames) {
            if (!fileName.EndsWith(suffix)) {
                continue;
            }

            int id;
            try {
                id = ParseFileId(fileName, suffix);
            } catch (const std::exception&) {
                // Ignore, cf. logging above.
                continue;
            }

            if (id >= thresholdId) {
                continue;
            }

            auto fullFileName = NFS::CombinePaths(path, fileName);

            YT_LOG_INFO("Janitor is removing Hydra file (FileName: %v)",
                fullFileName);
            try {
                NFS::Remove(fullFileName);
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Janitor is unable to remove Hydra file (FileName: %v)",
                    fullFileName);
            }
        }
    }

    void OnCleanup()
    {
        auto snapshots = ListFiles(SnapshotPath_, "." + SnapshotExtension);
        auto changelogs = ListFiles(ChangelogPath_, "." + ChangelogExtension);

        auto thresholdId = ComputeJanitorThresholdId(
            snapshots,
            changelogs,
            GetConfig());

        RemoveFiles(SnapshotPath_, "." + SnapshotExtension, thresholdId);
        RemoveFiles(ChangelogPath_, "." + ChangelogExtension, thresholdId);
        RemoveFiles(ChangelogPath_, "." + ChangelogExtension + "." + ChangelogIndexExtension, thresholdId);
    }
};

////////////////////////////////////////////////////////////////////////////////

ILocalHydraJanitorPtr CreateLocalHydraJanitor(
    TString snapshotPath,
    TString changelogPath,
    TLocalHydraJanitorConfigPtr config,
    IInvokerPtr ioInvoker)
{
    return New<TLocalHydraJanitor>(
        std::move(snapshotPath),
        std::move(changelogPath),
        std::move(config),
        std::move(ioInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
