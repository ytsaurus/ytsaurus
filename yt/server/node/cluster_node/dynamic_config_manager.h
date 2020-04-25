#pragma once

#include "public.h"

#include <yt/core/actions/future.h>
#include <yt/core/actions/signal.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/error.h>

#include <yt/core/ytree/public.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages dynamic configuration of YT node
//! by pulling it periodically from masters.
/*!
 *  \note
 *  Thread affinity: Control (unless noted otherwise)
 */
class TDynamicConfigManager
    : public TRefCounted
{
public:
    //! Raises when dynamic config changes.
    DEFINE_SIGNAL(void(const TClusterNodeDynamicConfigPtr&), ConfigUpdated);

public:
    TDynamicConfigManager(
        TDynamicConfigManagerConfigPtr config,
        TBootstrap* bootstrap);

    void Start();

    TFuture<void> Stop();

    void PopulateAlerts(std::vector<TError>* errors);

    NYTree::IYPathServicePtr GetOrchidService();

    /*!
    *  \note
    *  Thread affinity: any
    */
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

    std::atomic<bool> ConfigLoaded_ = false;
};

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
