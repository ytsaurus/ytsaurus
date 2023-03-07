#pragma once

#include "public.h"

#include <yt/ytlib/job_prober_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/ytree/permission.h>

#include <yt/core/rpc/public.h>

#include <yt/core/actions/future.h>

namespace NYT::NJobProberClient {

////////////////////////////////////////////////////////////////////////////////

struct TJobNodeDescriptorKey
{
    TString User;
    NJobTrackerClient::TJobId JobId;
    NYTree::EPermissionSet Permissions;

    bool operator == (const TJobNodeDescriptorKey& other) const;
    bool operator != (const TJobNodeDescriptorKey& other) const;
    operator size_t() const;
};

void FormatValue(TStringBuilderBase* builder, const TJobNodeDescriptorKey& key, TStringBuf format);
TString ToString(const TJobNodeDescriptorKey& key);

////////////////////////////////////////////////////////////////////////////////

class TJobNodeDescriptorCache
    : public TRefCounted
{
public:
    TJobNodeDescriptorCache(
        TAsyncExpiringCacheConfigPtr config,
        NRpc::IChannelPtr schedulerChannel);

    ~TJobNodeDescriptorCache();

    TFuture<NNodeTrackerClient::TNodeDescriptor> Get(const TJobNodeDescriptorKey& key);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TJobNodeDescriptorCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT:::NJobProberClient

