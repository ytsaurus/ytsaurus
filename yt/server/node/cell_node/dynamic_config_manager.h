#pragma once

#include "public.h"

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/ytree/ypath_service.h>

namespace NYT::NCellNode {

////////////////////////////////////////////////////////////////////////////////

class TDynamicConfigManager
    : public TRefCounted
{
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
    bool TryFetchConfig();

    void DoBuildOrchid(NYson::IYsonConsumer* consumer);

    const TDynamicConfigManagerConfigPtr Config_;

    TBootstrap* Bootstrap_;

    const IInvokerPtr ControlInvoker_;
    const NConcurrency::TPeriodicExecutorPtr Executor_;

    NYTree::INodePtr CurrentConfig_;
    std::vector<TString> CurrentNodeTagList_;

    std::optional<TError> LastError_ = std::nullopt;
    std::optional<TError> LastUnrecognizedOptionError_ = std::nullopt;

    TInstant LastConfigUpdateTime_;

    std::atomic<bool> ConfigLoaded_ = {false};
};

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellNode
