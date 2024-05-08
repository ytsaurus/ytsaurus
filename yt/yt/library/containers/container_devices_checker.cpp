#include "container_devices_checker.h"

#include "config.h"
#include "instance.h"
#include "porto_executor.h"
#include "private.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/fs.h>

#include <util/random/random.h>

namespace NYT::NContainers {

using namespace NConcurrency;
using namespace NLogging;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

TContainerDevicesChecker::TContainerDevicesChecker(
    TString testDirectoryPath,
    TPortoExecutorDynamicConfigPtr config,
    IInvokerPtr invoker,
    TLogger logger)
    : TestDirectoryPath_(std::move(testDirectoryPath))
    , VolumesPath_(NFS::CombinePaths(TestDirectoryPath_, "volumes"))
    , LayersPath_(NFS::CombinePaths(TestDirectoryPath_, "porto_layers"))
    , PortoVolumesPath_(NFS::CombinePaths(TestDirectoryPath_, "porto_volumes"))
    , PortoStoragePath_ (NFS::CombinePaths(TestDirectoryPath_, "porto_storage"))
    , LockPath(NFS::CombinePaths(TestDirectoryPath_, "lock"))
    , Config_(std::move(config))
    , Logger(std::move(logger))
    , CheckInvoker_(std::move(invoker))
    , Executor_(CreatePortoExecutor(
        Config_,
        "container_devices_check"))
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        CheckInvoker_,
        BIND_NO_PROPAGATE(&TContainerDevicesChecker::OnCheck, MakeWeak(this)),
        Config_->ApiTimeout))
{ }

void TContainerDevicesChecker::Start()
{
    YT_VERIFY(!PeriodicExecutor_->IsStarted());
    PeriodicExecutor_->Start();
}

void TContainerDevicesChecker::OnDynamicConfigChanged(const TPortoExecutorDynamicConfigPtr& newConfig)
{
    YT_LOG_INFO(
        "Container devices checker dynamic config changed (EnableTestPortoFailures: %v, StubErrorCode: %v)",
        Config_->EnableTestPortoFailures,
        Config_->StubErrorCode);

    Executor_->OnDynamicConfigChanged(newConfig);
}

void TContainerDevicesChecker::OnCheck()
{
    YT_LOG_DEBUG("Run container devices check");

    try {
        auto result = CreateTestContainer();
        Check_.Fire(result);
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Container devices check failed");
    }
}

void TContainerDevicesChecker::PrepareDirectory()
{
    YT_LOG_INFO("Container devices checker started");

    NFS::MakeDirRecursive(TestDirectoryPath_, 0755);

    TFile lock(LockPath, CreateAlways | WrOnly | Seq | CloseOnExec);
    lock.Flock(LOCK_EX);

    // Volumes are not expected to be used since all jobs must be dead by now.
    auto volumePathsOrErros = WaitFor(Executor_->ListVolumePaths());

    if (!volumePathsOrErros.IsOK()) {
        YT_LOG_WARNING(volumePathsOrErros, "Container device checker start failed");
        return;
    }

    std::vector<TFuture<void>> unlinkFutures;
    for (const auto& volumePath : volumePathsOrErros.Value()) {
        if (volumePath.StartsWith(VolumesPath_)) {
            unlinkFutures.push_back(Executor_->UnlinkVolume(volumePath, "self"));
        }
    }

    auto unlinkResults = WaitFor(AllSet(unlinkFutures))
        .ValueOrThrow();

    for (const auto& unlinkError : unlinkResults) {
        if (!unlinkError.IsOK() && unlinkError.GetCode() != EPortoErrorCode::VolumeNotLinked &&
            unlinkError.GetCode() != EPortoErrorCode::VolumeNotFound)
        {
            YT_LOG_ERROR(unlinkError, "Remove existing volume failed");
        }
    }

    if (NFS::Exists(VolumesPath_)) {
        NFS::RemoveRecursive(VolumesPath_);
    }

    if (NFS::Exists(LayersPath_)) {
        NFS::RemoveRecursive(LayersPath_);
    }

    NFS::MakeDirRecursive(VolumesPath_, 0755);
    NFS::MakeDirRecursive(LayersPath_, 0755);
    NFS::MakeDirRecursive(PortoVolumesPath_, 0755);
    NFS::MakeDirRecursive(PortoStoragePath_ , 0755);

    RootContainerName_ = GetSelfContainerName(Executor_);
}

TError TContainerDevicesChecker::CreateTestContainer()
{
    if (!DirectoryPrepared_) {
        try {
            PrepareDirectory();
            DirectoryPrepared_ = true;
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Directory preparation failed");
            WaitFor(PeriodicExecutor_->Stop()).ThrowOnError();
        }
    }

    TFile lock(LockPath, CreateAlways | WrOnly | Seq | CloseOnExec);
    lock.Flock(LOCK_EX);

    auto containerName = Format("%v/test_container", RootContainerName_);
    auto volumePath = NFS::CombinePaths(VolumesPath_, "test_volume");
    auto mountPath = NFS::CombinePaths(volumePath, "mount");

    // Create rootfs volume.
    {
        if (NFS::Exists(mountPath)) {
            NFS::RemoveRecursive(mountPath);
        }

        NFS::MakeDirRecursive(mountPath, 0755);

        THashMap<TString, TString> volumeProperties = {
            {"backend", "overlay"},
            {"place", TestDirectoryPath_},
            {"layers", mountPath}
        };

        auto createVolumeResult = WaitFor(Executor_->CreateVolume(mountPath, volumeProperties));

        if (createVolumeResult.IsOK() ||
            createVolumeResult.FindMatching(EPortoErrorCode::VolumeAlreadyExists) ||
            createVolumeResult.FindMatching(EPortoErrorCode::VolumeAlreadyLinked))
        {
            YT_VERIFY(!createVolumeResult.IsOK() || createVolumeResult.Value() == mountPath);
            YT_LOG_DEBUG("Test volume created (VolumePath: %v)", mountPath);
        } else {
            YT_LOG_DEBUG(createVolumeResult, "Test volume creation finished with error");
            return {};
        }
    }

    auto launcher = CreatePortoInstanceLauncher(containerName, Executor_);

    // Set container spec.
    {
        auto portoUserOrError = WaitFor(Executor_->GetContainerProperty(RootContainerName_, "user"));

        if (!portoUserOrError.IsOK() || !portoUserOrError.Value().has_value()) {
            YT_LOG_DEBUG(portoUserOrError, "Failed to get porto user");
            return {};
        }

        launcher->SetUser(*portoUserOrError.Value());
        launcher->SetRoot(TRootFS{
            .RootPath = mountPath,
            .IsRootReadOnly = false
        });
        launcher->SetDevices({});
        launcher->DisableNetwork();
        launcher->SetEnablePorto(EEnablePorto::None);
        launcher->SetIsolate(true);
    }

    auto result = WaitFor(launcher->LaunchMeta({}));

    YT_LOG_DEBUG_IF(!result.IsOK(), result, "Test container creation failed");

    try {
        // Cleanup leftovers during restart.
        WaitFor(Executor_->DestroyContainer(containerName))
            .ThrowOnError();
    } catch (const TErrorException& ex) {
        // If container doesn't exist it's OK.
        if (!ex.Error().FindMatching(EPortoErrorCode::ContainerDoesNotExist)) {
            YT_LOG_WARNING(ex, "Test container remove failed");
        }
    }

    {
        // Cleanup leftovers during restart.
        auto removeVolumeError = WaitFor(Executor_->UnlinkVolume(mountPath, "self"));

        if (removeVolumeError.FindMatching(EPortoErrorCode::VolumeNotLinked) ||
            removeVolumeError.FindMatching(EPortoErrorCode::VolumeNotFound))
        {
            YT_LOG_WARNING(removeVolumeError, "Test volume remove failed");
        }

        if (NFS::Exists(mountPath)) {
            NFS::RemoveRecursive(mountPath);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TContainerDevicesCheckerPtr CreateContainerDevicesChecker(
    TString testDirectoryPath,
    TPortoExecutorDynamicConfigPtr config,
    IInvokerPtr invoker,
    NLogging::TLogger logger)
{
    return New<TContainerDevicesChecker>(
        std::move(testDirectoryPath),
        std::move(config),
        std::move(invoker),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

#else

TContainerDevicesCheckerPtr CreateContainerDevicesChecker(
    TString /*testDirectoryPath*/,
    TPortoExecutorDynamicConfigPtr /*config*/,
    IInvokerPtr /*invoker*/,
    NLogging::TLogger /*logger*/)
{
    THROW_ERROR_EXCEPTION("Container devices checker is not available on this platform");
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
