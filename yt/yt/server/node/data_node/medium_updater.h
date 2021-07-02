#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TMediumUpdater
    : public TRefCounted
{
public:
    explicit TMediumUpdater(IBootstrap* bootstrap);

    void Start();

    TFuture<void> Stop();
    std::optional<TString> GetMediumOverride(TLocationUuid locationUuid) const;

    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& oldNodeConfig,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newNodeConfig);

private:
    void DoFetchConfig();
    void TryFetchConfig();

    void UpdateMedia();

    IBootstrap* const Bootstrap_;
    const IInvokerPtr ControlInvoker_;
    NConcurrency::TPeriodicExecutorPtr Executor_;
    bool Enabled_;

    using TMediumOverrideMap = THashMap<NChunkClient::TLocationUuid, TString>;

    TMediumOverrideMap MediumOverrides_;
};

DEFINE_REFCOUNTED_TYPE(TMediumUpdater)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
