#include "job_shell_descriptor_cache.h"
#include "private.h"

#include <yt/yt/ytlib/scheduler/job_prober_service_proxy.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/misc/async_expiring_cache.h>

#include <library/cpp/yt/misc/hash.h>

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
        NRpc::IChannelPtr schedulerChannel)
        : TAsyncExpiringCache(std::move(config))
        , JobProberProxy_(std::move(schedulerChannel))
    { }

private:
    TJobProberServiceProxy JobProberProxy_;

    TFuture<TJobShellDescriptor> DoGet(
        const TJobShellDescriptorKey& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        YT_LOG_DEBUG("Requesting job shell descriptor from scheduler (Key: %v)",
            key);

        auto req = JobProberProxy_.GetJobShellDescriptor();
        req->SetUser(key.User);
        ToProto(req->mutable_job_id(), key.JobId);
        if (key.ShellName) {
            req->set_shell_name(*key.ShellName);
        }

        return req->Invoke().Apply(BIND([=] (const TJobProberServiceProxy::TErrorOrRspGetJobShellDescriptorPtr& rspOrError) {
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
