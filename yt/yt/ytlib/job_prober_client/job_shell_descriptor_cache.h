#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/job_prober_client/public.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/ytree/permission.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NJobProberClient {

////////////////////////////////////////////////////////////////////////////////

struct TJobShellDescriptorKey
{
    TString User;
    NJobTrackerClient::TJobId JobId;
    std::optional<TString> ShellName;

    bool operator==(const TJobShellDescriptorKey& other) const = default;

    operator size_t() const;
};

void FormatValue(TStringBuilderBase* builder, const TJobShellDescriptorKey& key, TStringBuf format);
TString ToString(const TJobShellDescriptorKey& key);

////////////////////////////////////////////////////////////////////////////////

struct TJobShellDescriptor
{
    NNodeTrackerClient::TNodeDescriptor NodeDescriptor;

    TString Subcontainer;
};

void FormatValue(TStringBuilderBase* builder, const TJobShellDescriptor& descriptor, TStringBuf format);
TString ToString(const TJobShellDescriptor& descriptor);

////////////////////////////////////////////////////////////////////////////////

class TJobShellDescriptorCache
    : public TRefCounted
{
public:
    TJobShellDescriptorCache(
        TAsyncExpiringCacheConfigPtr config,
        TWeakPtr<NApi::NNative::IConnection> connection,
        NNodeTrackerClient::INodeChannelFactoryPtr channelFactory);

    ~TJobShellDescriptorCache();

    TFuture<TJobShellDescriptor> Get(const TJobShellDescriptorKey& key);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TJobShellDescriptorCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT:::NJobProberClient

