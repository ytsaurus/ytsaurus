#include "chaos_manager_cell_directory_synchronizer.h"

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NChaosServer {

using namespace NHiveClient;
using namespace NLogging;
using namespace NConcurrency;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

namespace {

DEFINE_ENUM(EIterationResult,
    ((ShouldStop)   (0))
    ((Continue)     (1))
    ((Error)        (2))
);

void AddOrUpdateCellDescriptor(TCellDescriptor descriptor, THashMap<TChaosCellId, TCellDescriptor>* cellDescriptorsMap)
{
    // Duplicates are possible.
    auto [it, inserted] = cellDescriptorsMap->try_emplace(descriptor.CellId, std::move(descriptor));
    if (!inserted && it->second.ConfigVersion < descriptor.ConfigVersion) {
        it->second = std::move(descriptor);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TChaosManagerCellDirectorySynchronizer
    : public IChaosManagerCellDirectorySynchronizer
{
public:
    TChaosManagerCellDirectorySynchronizer(
        ICellDirectoryPtr cellDirectory,
        TLogger logger,
        TDuration errorDelay)
        : Logger(std::move(logger))
        , CellDirectory_(std::move(cellDirectory))
        , ErrorDelay_(errorDelay)
    { }

    ~TChaosManagerCellDirectorySynchronizer()
    {
        if (SyncFuture_) {
            SyncFuture_.Cancel(TError("Synchronizer is stopped"));
        }
    }

    void AddCellDescriptor(TCellDescriptor descriptor) override
    {
        auto guard = Guard(SpinLock_);
        DoAddCellDescriptor(std::move(descriptor));
        StartIfNeeded();
    }

    void AddCellDescriptors(std::vector<TCellDescriptor> descriptors) override
    {
        if (descriptors.empty()) {
            return;
        }

        auto guard = Guard(SpinLock_);
        for (auto& descriptor : descriptors) {
            DoAddCellDescriptor(std::move(descriptor));
        }

        StartIfNeeded();
    }

    void Reconfigure(TDuration errorDelay) override
    {
        ErrorDelay_.store(errorDelay);
    }

private:
    const TLogger Logger;
    const ICellDirectoryPtr CellDirectory_;

    std::atomic<TDuration> ErrorDelay_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, SpinLock_);
    TFuture<void> SyncFuture_;
    THashMap<TChaosCellId, TCellDescriptor> CellDescriptors_;
    bool IsRunning_ = false;

    void DoAddCellDescriptor(TCellDescriptor descriptor)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

        AddOrUpdateCellDescriptor(std::move(descriptor), &CellDescriptors_);
    }

    void StartIfNeeded()
    {
        YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

        if (!IsRunning_) {
            IsRunning_ = true;
            SyncFuture_ = BIND(&TChaosManagerCellDirectorySynchronizer::Sync, MakeWeak(this))
                .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
                .Run();
        }
    }

    static void Sync(TWeakPtr<TChaosManagerCellDirectorySynchronizer> weakSelf)
    {
        THashMap<TChaosCellId, TCellDescriptor> cellDescriptorsMap;

        while (true) {
            auto strongSelf = weakSelf.Lock();
            if (!strongSelf) {
                return;
            }

            auto result = strongSelf->SyncIteration(&cellDescriptorsMap);
            if (result == EIterationResult::ShouldStop) {
                return;
            }

            auto errorDelay = strongSelf->ErrorDelay_.load();
            strongSelf.Reset();

            if (result == EIterationResult::Error) {
                TDelayedExecutor::WaitForDuration(errorDelay);
            }
        }
    }

    EIterationResult SyncIteration(THashMap<TChaosCellId, TCellDescriptor>* cellDescriptorsMap)
    {
        YT_TLOG_DEBUG("Start chaos manager cell directory synchronizer iteration");

        int previousIterationCellCount = std::ssize(*cellDescriptorsMap);
        int incomingCellCount = -1;

        {
            auto guard = Guard(SpinLock_);
            incomingCellCount = CellDescriptors_.size();

            if (cellDescriptorsMap->empty()) {
                *cellDescriptorsMap = std::move(CellDescriptors_);
            } else {
                for (auto& [_, descriptor] : CellDescriptors_) {
                    AddOrUpdateCellDescriptor(std::move(descriptor), cellDescriptorsMap);
                }
            }

            CellDescriptors_.clear();

            if (cellDescriptorsMap->empty()) {
                YT_TLOG_DEBUG("No cells to synchronize; exiting");

                IsRunning_ = false;
                return EIterationResult::ShouldStop;
            }
        }

        int currentIterationCellCount = cellDescriptorsMap->size();
        int errorCount = 0;

        auto result = EIterationResult::Continue;
        for (auto it = cellDescriptorsMap->begin(); it != cellDescriptorsMap->end();) {
            try {
                CellDirectory_->ReconfigureCell(it->second);
                cellDescriptorsMap->erase(it++);
            } catch (const std::exception& ex) {
                result = EIterationResult::Error;
                ++errorCount;

                YT_TLOG_DEBUG("Error synchronizing chaos cell directory")
                    .With("CellId", it->first)
                    .With(ex);

                ++it;
            }
        }

        YT_TLOG_DEBUG("Finish chaos manager cell directory synchronizer iteration")
            .With("PreviousIterationCellCount", previousIterationCellCount)
            .With("IncomingCellCount", incomingCellCount)
            .With("CurrentIterationCellCount", currentIterationCellCount)
            .With("ErrorCount", errorCount)
            .With("IterationResult", result);

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

IChaosManagerCellDirectorySynchronizerPtr CreateChaosManagerCellDirectorySynchronizer(
    ICellDirectoryPtr cellDirectory,
    TLogger logger,
    TDuration errorDelay)
{
    return New<TChaosManagerCellDirectorySynchronizer>(
        std::move(cellDirectory),
        std::move(logger),
        errorDelay);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
