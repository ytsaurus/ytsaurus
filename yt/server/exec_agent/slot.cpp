#include "slot.h"
#include "private.h"
#include "config.h"
#include "job_environment.h"
#include "slot_location.h"

#include <yt/ytlib/job_prober_client/job_prober_service_proxy.h>
#include <yt/ytlib/cgroup/cgroup.h>

#include <yt/core/bus/tcp_client.h>

#include <yt/core/rpc/bus_channel.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/proc.h>

#include <yt/core/tools/tools.h>

#include <util/folder/dirut.h>

#include <util/system/fs.h>

namespace NYT {
namespace NExecAgent {

using namespace NJobProberClient;
using namespace NConcurrency;
using namespace NBus;
using namespace NRpc;
using namespace NTools;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TSlot
    : public ISlot
{
public:
    TSlot(int slotIndex, TSlotLocationPtr location, IJobEnvironmentPtr environment, const Stroka& nodeTag)
        : SlotIndex_(slotIndex)
        , JobEnvironment_(std::move(environment))
        , Location_(std::move(location))
        , NodeTag_(nodeTag)
    {
        Location_->IncreaseSessionCount();
    }

    virtual void Cleanup() override
    {
        // First kill all processes that may hold open handles to slot directories.
        JobEnvironment_->CleanProcesses(SlotIndex_);

        // After that clean the filesystem.
        Location_->CleanSandboxes(SlotIndex_);
        Location_->DecreaseSessionCount();
    }

    virtual TFuture<void> RunJobProxy(
        NJobProxy::TJobProxyConfigPtr config,
        const TJobId& jobId,
        const TOperationId& operationId) override
    {
        try {
            Location_->MakeConfig(SlotIndex_, ConvertToNode(config));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to create job proxy config") << ex;
        }

        return JobEnvironment_->RunJobProxy(
            SlotIndex_,
            Location_->GetSlotPath(SlotIndex_),
            jobId,
            operationId);
    }

    virtual void MakeLink(
        ESandboxKind sandboxKind,
        const Stroka& targetPath,
        const Stroka& linkName,
        bool executable) override
    {
        Location_->MakeSandboxLink(SlotIndex_, sandboxKind, targetPath, linkName, executable);
    }

    virtual void MakeCopy(
        ESandboxKind sandboxKind,
        const Stroka& sourcePath,
        const Stroka& destinationName,
        bool executable) override
    {
        Location_->MakeSandboxCopy(SlotIndex_, sandboxKind, sourcePath, destinationName, executable);
    }

    virtual Stroka PrepareTmpfs(
        ESandboxKind sandboxKind,
        i64 size,
        Stroka path) override
    {
        return Location_->MakeSandboxTmpfs(
            SlotIndex_,
            sandboxKind,
            size,
            JobEnvironment_->GetUserId(SlotIndex_),
            path);
    }

    virtual TJobProberServiceProxy GetJobProberProxy() override
    {
        if (!JobProberProxy_) {
            auto client = CreateTcpBusClient(GetRpcClientConfig());
            auto channel = CreateBusChannel(std::move(client));
            JobProberProxy_.Emplace(std::move(channel));
        }

        return *JobProberProxy_;
    }

    virtual int GetSlotIndex() const override
    {
        return SlotIndex_;
    }

    virtual TTcpBusServerConfigPtr GetRpcServerConfig() const override
    {
        auto unixDomainName = Format("%v-job-proxy-%v", NodeTag_, SlotIndex_);
        return TTcpBusServerConfig::CreateUnixDomain(unixDomainName);
    }

    void Initialize()
    {
        Location_->CreateSandboxDirectories(SlotIndex_);
    }

private:
    const int SlotIndex_;
    IJobEnvironmentPtr JobEnvironment_;
    TSlotLocationPtr Location_;

    //! Uniquely identifies a node process on the current host.
    //! Used for unix socket name generation, to communicate between node and job proxies.
    const Stroka NodeTag_;

    TNullable<TJobProberServiceProxy> JobProberProxy_;


    TTcpBusClientConfigPtr GetRpcClientConfig() const
    {
        auto unixDomainName = Format("%v-job-proxy-%v", NodeTag_, SlotIndex_);
        return TTcpBusClientConfig::CreateUnixDomain(unixDomainName);
    }
};

////////////////////////////////////////////////////////////////////////////////

ISlotPtr CreateSlot(
    int slotIndex,
    TSlotLocationPtr location,
    IJobEnvironmentPtr environment,
    const Stroka& nodeTag)
{
    auto slot = New<TSlot>(
        slotIndex,
        std::move(location),
        std::move(environment),
        nodeTag);

    slot->Initialize();
    return slot;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
