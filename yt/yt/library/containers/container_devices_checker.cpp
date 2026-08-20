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
    std::string testDirectoryPath,
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
    YT_TLOG_INFO("Container devices checker dynamic config changed")
        .With("EnableTestPortoFailures", Config_->EnableTestPortoFailures)
        .With("StubErrorCode", Config_->StubErrorCode);

    Executor_->OnDynamicConfigChanged(newConfig);
}

void TContainerDevicesChecker::OnCheck()
{
    YT_TLOG_DEBUG("Run container devices check");

    try {
        auto result = CreateTestContainer();
        Check_.Fire(result);
    } catch (const std::exception& ex) {
        YT_TLOG_ERROR("Container devices check failed")
            .With(ex);
    }
}

void TContainerDevicesChecker::PrepareDirectory()
{
    YT_TLOG_INFO("Container devices checker started");

    NFS::MakeDirRecursive(TestDirectoryPath_, 0755);

    TFile lock(LockPath, CreateAlways | WrOnly | Seq | CloseOnExec);
    lock.Flock(LOCK_EX);

    // Volumes are not expected to be used since all jobs must be dead by now.
    auto volumePathsOrErros = WaitFor(Executor_->ListVolumePaths());

    if (!volumePathsOrErros.IsOK()) {
        YT_TLOG_WARNING("Container device checker start failed")
            .With(volumePathsOrErros);
        return;
    }

    std::vector<TFuture<void>> unlinkFutures;
    for (const auto& volumePath : volumePathsOrErros.Value()) {
        if (volumePath.starts_with(VolumesPath_)) {
            unlinkFutures.push_back(Executor_->UnlinkVolume(volumePath, "self"));
        }
    }

    auto unlinkResults = WaitFor(AllSet(unlinkFutures))
        .ValueOrThrow();

    for (const auto& unlinkError : unlinkResults) {
        if (!unlinkError.IsOK() && unlinkError.GetCode() != EPortoErrorCode::VolumeNotLinked &&
            unlinkError.GetCode() != EPortoErrorCode::VolumeNotFound)
        {
            YT_TLOG_ERROR("Remove existing volume failed")
                .With(unlinkError);
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
            YT_TLOG_ERROR("Directory preparation failed")
                .With(ex);
            WaitFor(PeriodicExecutor_->Stop()).ThrowOnError();
        }
    }

    TFile lock(LockPath, CreateAlways | WrOnly | Seq | CloseOnExec);
    lock.Flock(LOCK_EX);

    auto containerName = Format("%v/test_container", RootContainerName_);
    auto volumePath = NFS::CombinePaths(VolumesPath_, "test_volume");
    std::string mountPath = NFS::CombinePaths(volumePath, "mount");

    // Create rootfs volume.
    {
        if (NFS::Exists(mountPath)) {
            NFS::RemoveRecursive(mountPath);
        }

        NFS::MakeDirRecursive(mountPath, 0755);

        THashMap<std::string, std::string> volumeProperties = {
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
            YT_TLOG_DEBUG("Test volume created")
                .With("VolumePath", mountPath);
        } else {
            YT_TLOG_DEBUG("Test volume creation finished with error")
                .With(createVolumeResult);
            return {};
        }
    }

    auto launcher = CreatePortoInstanceLauncher(containerName, Executor_);

    // Set container spec.
    {
        auto portoUserOrError = WaitFor(Executor_->GetContainerProperty(RootContainerName_, "user"));

        if (!portoUserOrError.IsOK() || !portoUserOrError.Value().has_value()) {
            YT_TLOG_DEBUG("Failed to get Porto user")
                .With(portoUserOrError);
            return {};
        }

        launcher->SetUser(*portoUserOrError.Value());
        launcher->SetRoot(TRootFS{
            .RootPath = mountPath,
            .IsRootReadOnly = false,
        });
        launcher->SetDevices({});
        launcher->DisableNetwork();
        launcher->SetEnablePorto(EEnablePorto::None);
        launcher->SetIsolate(true);
    }

    auto result = WaitFor(launcher->LaunchMeta({}));

    YT_TLOG_DEBUG_IF(!result.IsOK(), "Test container creation failed")
        .With(result);

    try {
        // Cleanup leftovers during restart.
        WaitFor(Executor_->DestroyContainer(containerName))
            .ThrowOnError();
    } catch (const TErrorException& ex) {
        // If container doesn't exist it's OK.
        if (!ex.Error().FindMatching(EPortoErrorCode::ContainerDoesNotExist)) {
            YT_TLOG_WARNING("Test container remove failed")
                .With(ex);
        }
    }

    {
        // Cleanup leftovers during restart.
        auto removeVolumeError = WaitFor(Executor_->UnlinkVolume(mountPath, "self"));

        if (removeVolumeError.FindMatching(EPortoErrorCode::VolumeNotLinked) ||
            removeVolumeError.FindMatching(EPortoErrorCode::VolumeNotFound))
        {
            YT_TLOG_WARNING("Test volume remove failed")
                .With(removeVolumeError);
        }

        if (NFS::Exists(mountPath)) {
            NFS::RemoveRecursive(mountPath);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TContainerDevicesCheckerPtr CreateContainerDevicesChecker(
    std::string testDirectoryPath,
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
    std::string /*testDirectoryPath*/,
    TPortoExecutorDynamicConfigPtr /*config*/,
    IInvokerPtr /*invoker*/,
    NLogging::TLogger /*logger*/)
{
    THROW_ERROR_EXCEPTION("Container devices checker is not available on this platform");
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
