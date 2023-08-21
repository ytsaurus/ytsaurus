#pragma once

#include "config.h"

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NDynamicConfig {

////////////////////////////////////////////////////////////////////////////////

//! Manages dynamic configuration of a server component
//! by pulling it periodically from masters.
/*!
 *  \note
 *  Thread affinity: any
 */
template <typename TConfig>
class TDynamicConfigManagerBase
    : public TRefCounted
{
public:
    using TConfigPtr = TIntrusivePtr<TConfig>;

public:
    //! Raises when dynamic config changes.
    DEFINE_SIGNAL(void(const TConfigPtr& /*oldConfig*/, const TConfigPtr& /*newConfig*/), ConfigChanged);

public:
    // NB: Invoker must be serialized.
    TDynamicConfigManagerBase(
        TDynamicConfigManagerOptions options,
        TDynamicConfigManagerConfigPtr config,
        NApi::IClientPtr client,
        IInvokerPtr invoker,
        NYTree::INodePtr baseConfigNode = nullptr);

    explicit TDynamicConfigManagerBase(
        TConfigPtr staticConfig);

    //! Starts the dynamic config manager.
    void Start();

    //! Returns the list of last config update attempt errors.
    std::vector<TError> GetErrors() const;

    //! Returns orchid with config and last config update time.
    NYTree::IYPathServicePtr GetOrchidService() const;

    //! Returns |true| if dynamic config was loaded successfully
    //! at least once.
    bool IsConfigLoaded() const;

    //! Returns the current dynamic config as a node.
    NYTree::IMapNodePtr GetConfigNode() const;

    //! Returns the current dynamic config as a deserialized instance.
    TConfigPtr GetConfig() const;

    //! Returns the initial dynamic config as a deserialized instance.
    TConfigPtr GetInitialConfig() const;

    //! Returns a future that becomes set when dynamic config
    //! is loaded for the first time.
    TFuture<void> GetConfigLoadedFuture() const;

protected:
    //! Returns the list of instance tags.
    virtual std::vector<TString> GetInstanceTags() const;

private:
    const TDynamicConfigManagerOptions Options_;
    const TDynamicConfigManagerConfigPtr Config_;

    const NApi::IClientPtr Client_;

    const IInvokerPtr Invoker_;
    const NYTree::INodePtr BaseConfigNode_;
    const NConcurrency::TPeriodicExecutorPtr UpdateExecutor_;

    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    TError UpdateError_;
    TError UnrecognizedOptionError_;
    TInstant LastConfigUpdateTime_;
    TInstant LastConfigChangeTime_;
    NYTree::IMapNodePtr AppliedConfigNode_;
    TConfigPtr AppliedConfig_;
    TConfigPtr InitialConfig_;

    std::vector<TString> InstanceTags_;

    //! This promise becomes set when dynamic config was loaded
    //! for the first time.
    const TPromise<void> ConfigLoadedPromise_ = NewPromise<void>();

    void DoUpdateConfig();

    //! Returns |true| if config was actually updated.
    //! Throws on error.
    bool TryUpdateConfig();

    void DoBuildOrchid(NYson::IYsonConsumer* consumer) const;

    //! Returns the list of last config update attempt errors when spinlock is already guarded.
    std::vector<TError> LockedGetErrors(const TGuard<NThreading::TSpinLock>& guard) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDynamicConfig

#define DYNAMIC_CONFIG_MANAGER_INL_H
#include "dynamic_config_manager-inl.h"
#undef DYNAMIC_CONFIG_MANAGER_INL_H
