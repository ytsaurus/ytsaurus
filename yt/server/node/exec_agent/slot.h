#pragma once

#include "public.h"

#include <yt/server/node/data_node/artifact.h>
#include <yt/server/node/data_node/public.h>

#include <yt/server/lib/containers/public.h>
#include <yt/server/lib/job_proxy/config.h>

#include <yt/ytlib/cgroup/cgroup.h>

#include <yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/client/formats/format.h>

#include <yt/core/actions/public.h>

#include <yt/core/bus/public.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/fs.h>

#include <util/stream/file.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

struct ISlot
    : public virtual TRefCounted
{
    //! Kill all possibly running processes and clean sandboxes.
    virtual void CleanProcesses() = 0;

    virtual void CleanSandbox() = 0;

    virtual void CancelPreparation() = 0;

    virtual TFuture<void> RunJobProxy(
        NJobProxy::TJobProxyConfigPtr config,
        TJobId jobId,
        TOperationId operationId) = 0;

    //! Returns tmpfs path if any.
    virtual TFuture<std::vector<TString>> CreateSandboxDirectories(const TUserSandboxOptions& options) = 0;

    virtual TFuture<void> MakeLink(
        ESandboxKind sandboxKind,
        const TString& targetPath,
        const TString& linkName,
        bool executable) = 0;

    virtual TFuture<void> MakeCopy(
        ESandboxKind sandboxKind,
        const TString& sourcePath,
        const TString& destinationName,
        bool executable) = 0;

    virtual TFuture<void> MakeFile(
        ESandboxKind sandboxKind,
        const std::function<void(IOutputStream*)>& producer,
        const TString& destinationName,
        bool executable) = 0;

    virtual TFuture<NDataNode::IVolumePtr> PrepareRootVolume(const std::vector<NDataNode::TArtifactKey>& layers) = 0;

    virtual TFuture<void> FinalizePreparation() = 0;

    virtual NBus::TTcpBusServerConfigPtr GetBusServerConfig() const = 0;
    virtual NBus::TTcpBusClientConfigPtr GetBusClientConfig() const = 0;

    virtual int GetSlotIndex() const = 0;

    virtual TString GetSandboxPath(ESandboxKind sandbox) const = 0;

    virtual TFuture<void> RunSetupCommands(
        TJobId jobId,
        const std::vector<NJobAgent::TShellCommandConfigPtr>& commands,
        const NContainers::TRootFS& rootFS,
        const TString& user) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISlot)

////////////////////////////////////////////////////////////////////////////////

ISlotPtr CreateSlot(
    int slotIndex,
    TSlotLocationPtr location,
    IJobEnvironmentPtr environment,
    NDataNode::IVolumeManagerPtr volumeManager,
    const TString& nodeTag);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
