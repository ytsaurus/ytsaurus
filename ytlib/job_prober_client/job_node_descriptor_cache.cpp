#include "job_node_descriptor_cache.h"
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

bool TJobNodeDescriptorKey::operator == (const TJobNodeDescriptorKey& other) const
{
    return
        User == other.User &&
        JobId == other.JobId &&
        Permissions == other.Permissions;
}

bool TJobNodeDescriptorKey::operator != (const TJobNodeDescriptorKey& other) const
{
    return !(*this == other);
}

TJobNodeDescriptorKey::operator size_t() const
{
    size_t result = 0;
    HashCombine(result, User);
    HashCombine(result, JobId);
    HashCombine(result, Permissions);
    return result;
}

void FormatValue(TStringBuilderBase* builder, const TJobNodeDescriptorKey& key, TStringBuf /*format*/)
{
    builder->AppendFormat("{User: %v, JobId: %v, Permissions: %v}",
        key.User,
        key.JobId,
        key.Permissions);
}

TString ToString(const TJobNodeDescriptorKey& key)
{
    return ToStringViaBuilder(key);
}

////////////////////////////////////////////////////////////////////////////////

class TJobNodeDescriptorCache::TImpl
    : public TAsyncExpiringCache<TJobNodeDescriptorKey, TNodeDescriptor>
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

    virtual TFuture<TNodeDescriptor> DoGet(
        const TJobNodeDescriptorKey& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        YT_LOG_DEBUG("Requesting job node descriptor from scheduler (Key: %v)",
            key);

        auto req = JobProberProxy_.GetJobNode();
        req->SetUser(key.User);
        ToProto(req->mutable_job_id(), key.JobId);
        req->set_required_permissions(static_cast<ui32>(key.Permissions));

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
            auto descriptor = FromProto<TNodeDescriptor>(rsp->node_descriptor());

            YT_LOG_DEBUG("Job node descriptor received (Key: %v, Descriptor: %v)",
                key,
                descriptor);

            return descriptor;
        }));
    }
};

////////////////////////////////////////////////////////////////////////////////

TJobNodeDescriptorCache::TJobNodeDescriptorCache(
    TAsyncExpiringCacheConfigPtr config,
    NRpc::IChannelPtr schedulerChannel)
    : Impl_(New<TImpl>(
        std::move(config),
        std::move(schedulerChannel)))
{ }

TJobNodeDescriptorCache::~TJobNodeDescriptorCache()
{ }

TFuture<TNodeDescriptor> TJobNodeDescriptorCache::Get(const TJobNodeDescriptorKey& key)
{
    return Impl_->Get(key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProberClient::NYT
