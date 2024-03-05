#include "job_shell_descriptor_cache.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/controller_agent/job_prober_service_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/scheduler/config.h>
#include <yt/yt/ytlib/scheduler/helpers.h>
#include <yt/yt/ytlib/scheduler/scheduler_service_proxy.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/misc/async_expiring_cache.h>
#include <yt/yt/core/misc/backoff_strategy.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <library/cpp/yt/misc/hash.h>

namespace NYT::NJobProberClient {

using namespace NRpc;
using namespace NYTree;
using namespace NNodeTrackerClient;
using namespace NScheduler;
using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProberClientLogger;

////////////////////////////////////////////////////////////////////////////////

TJobShellDescriptorKey::operator size_t() const
{
    size_t result = 0;
    HashCombine(result, User);
    HashCombine(result, JobId);
    HashCombine(result, ShellName);
    return result;
}

void FormatValue(TStringBuilderBase* builder, const TJobShellDescriptorKey& key, TStringBuf /*format*/)
{
    builder->AppendFormat("{User: %v, JobId: %v, ShellName: %v}",
        key.User,
        key.JobId,
        key.ShellName);
}

TString ToString(const TJobShellDescriptorKey& key)
{
    return ToStringViaBuilder(key);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TJobShellDescriptor& descriptor, TStringBuf /*format*/)
{
    builder->AppendFormat("{NodeDescriptor: %v, Subcontainer: %v}",
        descriptor.NodeDescriptor,
        descriptor.Subcontainer);
}

TString ToString(const TJobShellDescriptor& descriptor)
{
    return ToStringViaBuilder(descriptor);
}

////////////////////////////////////////////////////////////////////////////////

class TJobShellDescriptorCache::TImpl
    : public TAsyncExpiringCache<TJobShellDescriptorKey, TJobShellDescriptor>
{
public:
    TImpl(
        TAsyncExpiringCacheConfigPtr config,
        TWeakPtr<NApi::NNative::IConnection> connection,
        NNodeTrackerClient::INodeChannelFactoryPtr channelFactory)
        : TAsyncExpiringCache(std::move(config))
        , Connection_(std::move(connection))
        , ChannelFactory_(std::move(channelFactory))
    { }

private:
    TWeakPtr<NApi::NNative::IConnection> Connection_;
    NNodeTrackerClient::INodeChannelFactoryPtr ChannelFactory_;

    TFuture<TJobShellDescriptor> DoGet(
        const TJobShellDescriptorKey& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        auto connection = Connection_.Lock();
        YT_VERIFY(connection);

        return BIND(&TJobShellDescriptorCache::TImpl::RequestJobShellDescriptorFromControllerAgent, MakeStrong(this), key, connection)
            .AsyncVia(NRpc::TDispatcher::Get()->GetCompressionPoolInvoker())
            .Run();
    }

    TJobShellDescriptor RequestJobShellDescriptorFromControllerAgent(
        const TJobShellDescriptorKey& key,
        const NNative::IConnectionPtr& connection)
    {
        YT_LOG_DEBUG(
            "Requesting job shell descriptor from controller agent (Key: %v)",
            key);
        auto allocationId = NScheduler::AllocationIdFromJobId(key.JobId);

        YT_LOG_DEBUG(
            "Requesting allocation brief info (AllocationId: %v)",
            allocationId);

        auto allocationBriefInfo = WaitFor(NApi::NNative::GetAllocationBriefInfo(
            TOperationServiceProxy(connection->GetSchedulerChannel()),
            allocationId,
            NScheduler::TAllocationInfoToRequest{
                .OperationId = true,
                .OperationAcl = true,
                .ControllerAgentDescriptor = true,
                .NodeDescriptor = true,
            }))
            .ValueOrThrow();

        YT_LOG_DEBUG(
            "Allocation brief info received (AllocationId: %v)",
            allocationId);

        auto controllerAgentChannel = ChannelFactory_->CreateChannel(
            *allocationBriefInfo.ControllerAgentDescriptor.Addresses);

        NControllerAgent::TJobProberServiceProxy jobProberProxy(
            controllerAgentChannel);

        YT_LOG_DEBUG(
            "Getting job shell descriptor from agent (AllocationId: %v, OperationId: %v)",
            allocationId,
            allocationBriefInfo.OperationId);

        auto req = jobProberProxy.GetJobShellDescriptor();

        req->SetUser(key.User);
        ToProto(req->mutable_incarnation_id(), allocationBriefInfo.ControllerAgentDescriptor.IncarnationId);
        ToProto(req->mutable_operation_id(), allocationBriefInfo.OperationId);
        ToProto(req->mutable_job_id(), key.JobId);
        if (key.ShellName) {
            req->set_shell_name(*key.ShellName);
        }

        auto rspOrError = WaitFor(req->Invoke());

        if (!rspOrError.IsOK()) {
            if (NApi::NNative::IsRevivalError(rspOrError)) {
                THROW_ERROR_EXCEPTION("Failed to get job shell descriptor")
                    << NNative::MakeRevivalError(allocationBriefInfo.OperationId, key.JobId);
            }

            OnJobShellDescriptorFetchingFailed(std::move(rspOrError), key);
        }

        const auto& rsp = rspOrError.Value();

        TJobShellDescriptor jobShellDescriptor;
        jobShellDescriptor.NodeDescriptor = std::move(allocationBriefInfo.NodeDescriptor);
        jobShellDescriptor.Subcontainer = rsp->subcontainer();

        YT_LOG_DEBUG(
            "Job shell descriptor received (Key: %v, Descriptor: %v)",
            key,
            jobShellDescriptor);

        return jobShellDescriptor;
    }

    static void OnJobShellDescriptorFetchingFailed(
        TError error,
        const TJobShellDescriptorKey& key)
    {
        YT_VERIFY(!error.IsOK());

        YT_LOG_DEBUG(
            error,
            "Failed to get job shell descriptor (Key: %v)",
            key);
        THROW_ERROR_EXCEPTION("Failed to get job shell descriptor")
            << TErrorAttribute("user", key.User)
            << TErrorAttribute("job_id", key.JobId)
            << TErrorAttribute("shell_name", key.ShellName)
            << error;
    }
};

////////////////////////////////////////////////////////////////////////////////

TJobShellDescriptorCache::TJobShellDescriptorCache(
    TAsyncExpiringCacheConfigPtr config,
    TWeakPtr<NApi::NNative::IConnection> connection,
    NNodeTrackerClient::INodeChannelFactoryPtr channelFactory)
    : Impl_(New<TImpl>(
        std::move(config),
        std::move(connection),
        std::move(channelFactory)))
{ }

TJobShellDescriptorCache::~TJobShellDescriptorCache()
{ }

TFuture<TJobShellDescriptor> TJobShellDescriptorCache::Get(const TJobShellDescriptorKey& key)
{
    return Impl_->Get(key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProberClient::NYT
