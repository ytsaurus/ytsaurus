#include "job_shell_descriptor_cache.h"
#include "private.h"

#include <yt/ytlib/scheduler/job_prober_service_proxy.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/misc/async_expiring_cache.h>
#include <yt/core/misc/hash.h>

namespace NYT::NJobProberClient {

using namespace NRpc;
using namespace NYTree;
using namespace NNodeTrackerClient;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProberClientLogger;

////////////////////////////////////////////////////////////////////////////////

bool TJobShellDescriptorKey::operator == (const TJobShellDescriptorKey& other) const
{
    return
        User == other.User &&
        JobId == other.JobId &&
        ShellName == other.ShellName;
}

bool TJobShellDescriptorKey::operator != (const TJobShellDescriptorKey& other) const
{
    return !(*this == other);
}

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

void FormatValue(TStringBuilderBase* builder, const TJobShellDescriptor& descriptor, TStringBuf format)
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
        NRpc::IChannelPtr schedulerChannel)
        : TAsyncExpiringCache(std::move(config))
        , JobProberProxy_(std::move(schedulerChannel))
    { }

private:
    TJobProberServiceProxy JobProberProxy_;

    virtual TFuture<TJobShellDescriptor> DoGet(
        const TJobShellDescriptorKey& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        YT_LOG_DEBUG("Requesting job shell descriptor from scheduler (Key: %v)",
            key);

        // COMPAT(gritukan): Request job shell descriptors with null shell names
        // via `GetJobShellDescriptor' method too when schedulers will be fresh enough.

        if (key.ShellName) {
            auto req = JobProberProxy_.GetJobShellDescriptor();
            req->SetUser(key.User);
            ToProto(req->mutable_job_id(), key.JobId);
            req->set_shell_name(*key.ShellName);

            return req->Invoke().Apply(BIND([=, this_ = MakeStrong(this)] (const TJobProberServiceProxy::TErrorOrRspGetJobShellDescriptorPtr& rspOrError) {
                if (!rspOrError.IsOK()) {
                    YT_LOG_DEBUG(rspOrError, "Failed to get job shell descriptor (Key: %v)",
                        key);
                    THROW_ERROR_EXCEPTION("Failed to get job shell descriptor")
                        << TErrorAttribute("user", key.User)
                        << TErrorAttribute("job_id", key.JobId)
                        << TErrorAttribute("shell_name", key.ShellName)
                        << rspOrError;
                }

                const auto& rsp = rspOrError.Value();

                TJobShellDescriptor jobShellDescriptor;
                jobShellDescriptor.NodeDescriptor = FromProto<TNodeDescriptor>(rsp->node_descriptor());
                jobShellDescriptor.Subcontainer = rsp->subcontainer();

                YT_LOG_DEBUG("Job shell descriptor received (Key: %v, Descriptor: %v)",
                    key,
                    jobShellDescriptor);

                return jobShellDescriptor;
            }));
        } else {
            auto req = JobProberProxy_.GetJobNode();
            req->SetUser(key.User);
            ToProto(req->mutable_job_id(), key.JobId);
            auto requiredPermissions = EPermissionSet(EPermission::Manage | EPermission::Read);
            req->set_required_permissions(static_cast<ui32>(requiredPermissions));

            return req->Invoke().Apply(BIND([=, this_ = MakeStrong(this)] (const TJobProberServiceProxy::TErrorOrRspGetJobNodePtr& rspOrError) {
                if (!rspOrError.IsOK()) {
                    YT_LOG_DEBUG(rspOrError, "Failed to get job node descriptor (Key: %v)",
                        key);
                    THROW_ERROR_EXCEPTION("Failed to get job node descriptor")
                        << TErrorAttribute("user", key.User)
                        << TErrorAttribute("job_id", key.JobId)
                        << rspOrError;
                }

                const auto& rsp = rspOrError.Value();

                TJobShellDescriptor jobShellDescriptor;
                jobShellDescriptor.NodeDescriptor = FromProto<TNodeDescriptor>(rsp->node_descriptor());

                YT_LOG_DEBUG("Job node descriptor received (Key: %v, Descriptor: %v)",
                    key,
                    jobShellDescriptor.NodeDescriptor);

                return jobShellDescriptor;
            }));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TJobShellDescriptorCache::TJobShellDescriptorCache(
    TAsyncExpiringCacheConfigPtr config,
    NRpc::IChannelPtr schedulerChannel)
    : Impl_(New<TImpl>(
        std::move(config),
        std::move(schedulerChannel)))
{ }

TJobShellDescriptorCache::~TJobShellDescriptorCache()
{ }

TFuture<TJobShellDescriptor> TJobShellDescriptorCache::Get(const TJobShellDescriptorKey& key)
{
    return Impl_->Get(key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProberClient::NYT
