#pragma once

#include "public.h"

#include <yt/core/actions/future.h>
#include <yt/core/actions/signal.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/error.h>

#include <yt/core/ytree/public.h>

namespace NYT::NCellNode {

////////////////////////////////////////////////////////////////////////////////

class TDynamicConfigManager
    : public TRefCounted
{
public:
    //! Raises when dynamic config was updated.
    DEFINE_SIGNAL(void(TCellNodeDynamicConfigPtr), ConfigUpdated);

public:
    TDynamicConfigManager(
        TDynamicConfigManagerConfigPtr config,
        TBootstrap* bootstrap);

    void Start();

    TFuture<void> Stop();

    void PopulateAlerts(std::vector<TError>* errors);

    NYTree::IYPathServicePtr GetOrchidService();

    bool IsDynamicConfigLoaded() const;

private:
    void DoFetchConfig();
    void TryFetchConfig();

    void DoBuildOrchid(NYson::IYsonConsumer* consumer);

    const TDynamicConfigManagerConfigPtr Config_;

    TBootstrap* Bootstrap_;

    const IInvokerPtr ControlInvoker_;
    const NConcurrency::TPeriodicExecutorPtr Executor_;

    NYTree::INodePtr CurrentConfig_;
    std::vector<TString> CurrentNodeTagList_;

    TError LastError_;
    TError LastUnrecognizedOptionError_;

    TInstant LastConfigUpdateTime_;

    bool ConfigLoaded_ = false;
};

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellNode
