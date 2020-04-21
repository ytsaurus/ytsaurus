#include "slot.h"
#include "private.h"
#include "job_environment.h"
#include "slot_location.h"

#include <yt/server/lib/exec_agent/config.h>

#include <yt/server/node/data_node/volume_manager.h>

#include <yt/ytlib/cgroup/cgroup.h>

#include <yt/ytlib/tools/tools.h>

#include <yt/core/bus/tcp/client.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/proc.h>

#include <util/folder/dirut.h>

#include <util/system/fs.h>

namespace NYT::NExecAgent {

static NLogging::TLogger Logger("SlotLogger");

using namespace NBus;
using namespace NConcurrency;
using namespace NDataNode;
using namespace NTools;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TSlot
    : public ISlot
{
public:
    TSlot(
        int slotIndex,
        TSlotLocationPtr location,
        IJobEnvironmentPtr environment,
        IVolumeManagerPtr volumeManager,
        const TString& nodeTag)
        : SlotIndex_(slotIndex)
        , JobEnvironment_(std::move(environment))
        , Location_(std::move(location))
        , VolumeManager_(std::move(volumeManager))
        , NodeTag_(nodeTag)
        , JobProxyUnixDomainSocketPath_(GetJobProxyUnixDomainSocketPath())
    {
        Location_->IncreaseSessionCount();
    }

    virtual void CleanProcesses() override
    {
        // First kill all processes that may hold open handles to slot directories.
        JobEnvironment_->CleanProcesses(SlotIndex_);
    }

    virtual void CleanSandbox()
    {
        WaitFor(Location_->CleanSandboxes(
            SlotIndex_))
            .ThrowOnError();
        Location_->DecreaseSessionCount();
    }

    virtual void CancelPreparation() override
    {
        PreparationCanceled_ = true;

        for (const auto& future : PreparationFutures_) {
            future.Cancel(TError("Job preparation canceled"));
        }
    }

    virtual TFuture<void> RunJobProxy(
        NJobProxy::TJobProxyConfigPtr config,
        TJobId jobId,
        TOperationId operationId) override
    {
        return RunPrepareAction<void>([&] {
                {
                    auto error = WaitFor(Location_->MakeConfig(SlotIndex_, ConvertToNode(config)));
                    THROW_ERROR_EXCEPTION_IF_FAILED(error, "Failed to create job proxy config");
                }

                return JobEnvironment_->RunJobProxy(
                    SlotIndex_,
                    Location_->GetSlotPath(SlotIndex_),
                    jobId,
                    operationId);
            },
            // Job proxy preparation is uncancelable, otherwise we might try to kill
            // a never-started job proxy process.
            true);
    }

    virtual TFuture<void> MakeLink(
        ESandboxKind sandboxKind,
        const TString& targetPath,
        const TString& linkName,
        bool executable) override
    {
        return RunPrepareAction<void>([&] {
                return Location_->MakeSandboxLink(
                    SlotIndex_,
                    sandboxKind,
                    targetPath,
                    linkName,
                    executable);
            });
    }

    virtual TFuture<void> MakeCopy(
        ESandboxKind sandboxKind,
        const TString& sourcePath,
        const TString& destinationName,
        bool executable) override
    {
        return RunPrepareAction<void>([&] {
                return Location_->MakeSandboxCopy(
                    SlotIndex_,
                    sandboxKind,
                    sourcePath,
                    destinationName,
                    executable);
            });
    }

    virtual TFuture<void> MakeFile(
        ESandboxKind sandboxKind,
        const std::function<void(IOutputStream*)>& producer,
        const TString& destinationName,
        bool executable) override
    {
        return RunPrepareAction<void>([&] {
                return Location_->MakeSandboxFile(
                    SlotIndex_,
                    sandboxKind,
                    producer,
                    destinationName,
                    executable);
            });
    }

    virtual TFuture<void> FinalizePreparation() override
    {
        return RunPrepareAction<void>([&] {
                return Location_->FinalizeSandboxPreparation(
                    SlotIndex_,
                    JobEnvironment_->GetUserId(SlotIndex_));
            },
            // Permission setting is uncancelable since it includes tool invocation in a separate process.
            true);
    }

    virtual TFuture<IVolumePtr> PrepareRootVolume(const std::vector<TArtifactKey>& layers) override
    {
        if (!VolumeManager_) {
            return MakeFuture<IVolumePtr>(TError("Porto layers and custom root FS are not supported"));
        }
        return RunPrepareAction<IVolumePtr>([&] {
                return VolumeManager_->PrepareVolume(layers);
            });
    }

    virtual int GetSlotIndex() const override
    {
        return SlotIndex_;
    }

    virtual TString GetSandboxPath(ESandboxKind sandbox) const override
    {
        return Location_->GetSandboxPath(SlotIndex_, sandbox);
    }

    virtual TTcpBusServerConfigPtr GetBusServerConfig() const override
    {
        return TTcpBusServerConfig::CreateUnixDomain(JobProxyUnixDomainSocketPath_);
    }

    virtual TTcpBusClientConfigPtr GetBusClientConfig() const override
    {
        return TTcpBusClientConfig::CreateUnixDomain(JobProxyUnixDomainSocketPath_);
    }

    virtual TFuture<std::vector<TString>> CreateSandboxDirectories(const TUserSandboxOptions& options)
    {
        return RunPrepareAction<std::vector<TString>>([&] {
                return Location_->CreateSandboxDirectories(
                    SlotIndex_,
                    options,
                    JobEnvironment_->GetUserId(SlotIndex_));
            },
            // Includes quota setting and tmpfs creation.
            true);
    }

    virtual TFuture<void> RunSetupCommands(
        TJobId jobId,
        const std::vector<NJobAgent::TShellCommandConfigPtr>& commands,
        const NContainers::TRootFS& rootFS,
        const TString& user)
    {
        return RunPrepareAction<void>([&] {
                return JobEnvironment_->RunSetupCommands(SlotIndex_, jobId, commands, rootFS, user);
            },
            // Setup commands are uncancelable since they are run in separate processes.
            true);
    }

private:
    const int SlotIndex_;
    const IJobEnvironmentPtr JobEnvironment_;
    const TSlotLocationPtr Location_;
    IVolumeManagerPtr VolumeManager_;

    //! Uniquely identifies a node process on the current host.
    //! Used for unix socket name generation, to communicate between node and job proxies.
    const TString NodeTag_;

    std::vector<TFuture<void>> PreparationFutures_;
    bool PreparationCanceled_ = false;

    const TString JobProxyUnixDomainSocketPath_;

    template <class T>
    TFuture<T> RunPrepareAction(std::function<TFuture<T>()> action, bool uncancelable = false)
    {
        if (PreparationCanceled_) {
            return MakeFuture<T>(TError("Job preparation canceled")
                << TErrorAttribute("slot_index", SlotIndex_));
        } else {
            auto future = action();
            auto preparationFuture = future.template As<void>();
            PreparationFutures_.push_back(uncancelable
                ? preparationFuture.ToUncancelable()
                : preparationFuture);
            return future;
        }
    }

    TString GetJobProxyUnixDomainSocketPath() const
    {
        return NFS::GetRealPath(NFS::CombinePaths({
            Location_->GetSlotPath(SlotIndex_),
            "pipes",
            Format("%v-job-proxy-%v", NodeTag_, SlotIndex_)}));
    }
};

////////////////////////////////////////////////////////////////////////////////

ISlotPtr CreateSlot(
    int slotIndex,
    TSlotLocationPtr location,
    IJobEnvironmentPtr environment,
    IVolumeManagerPtr volumeManager,
    const TString& nodeTag)
{
    auto slot = New<TSlot>(
        slotIndex,
        std::move(location),
        std::move(environment),
        std::move(volumeManager),
        nodeTag);

    return slot;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
