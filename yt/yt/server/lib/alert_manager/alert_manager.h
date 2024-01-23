#pragma once

#include "public.h"

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NAlertManager {

////////////////////////////////////////////////////////////////////////////////

//! NB: Please read the comments below to get a picture of when error attribtutes should be specified in tags vs. the error itself.
struct TAlert
{
    //! Numeric error code corresponding to a YT error enum declaration.
    TErrorCode ErrorCode;
    //! Human-readable representation of the error code above.
    //! NB: Alerts with the same category name *must* have the same error code value.
    TString Category;
    //! Human-readable description of alert instance.
    //! It is possible to have different description variants for the same error code,
    //! but only one description can be used for a group of alerts at any moment in the alert manager's lifetime.
    TString Description;
    //! A set of tags to be added to the error as attributes *and* used in profiling.
    //! The intent of these tags is to distinguish between alerts within the same category.
    //! E.g. when the same operation is performed for different clusters, each alert within the category should be
    //! tagged with the cluster name. They will be grouped together under the same category. In profiling, the metrics
    //! would be parametrized by the category as well as the specified cluster name.
    //! NB: Therefore, alerts must be uniquely defined by their error code and tag set.
    NProfiling::TTagList Tags;
    //! Reported error.
    //! This error could have additional attributes that are diagnostically relevant but not needed as a profiling tag.
    TError Error;

    //! Returns the specified error with the specified tags attached as error attributes.
    TError GetTaggedError() const;
};

////////////////////////////////////////////////////////////////////////////////

//! Periodically collects alerts from subscribed components and groups them by specified alert categories.
//! To produce an alert for a category, all raw errors in this category are aggregated into inner errors of a single error
//! with the specified top-level error code and description.
//! Stored alerts can be exported via the provided orchid service.
//! It is recommended to utilize the collector interface below, but subscribers can also implement alert collection callback themselves.
//! NB: Subscribers must guarantee that alerts have a unique error code + tag set combination.
//! NB: It is recommended to submit alerts with error codes from a single YT error enum within one alert manager.
//!
//! Thread affinity: any.
struct IAlertManager
    : public TRefCounted
{
    virtual void Start() = 0;

    virtual void Reconfigure(
        const TAlertManagerDynamicConfigPtr& oldConfig,
        const TAlertManagerDynamicConfigPtr& newConfig) = 0;

    //! Returns an orchid service representing a snapshot of the stored alerts.
    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;
    //! Returns a snapshot of managed alerts.
    virtual THashMap<TString, TError> GetAlerts() const = 0;

    virtual NLogging::TLogger GetLogger() const = 0;
    virtual NProfiling::TProfiler GetAlertProfiler() const = 0;

    DECLARE_INTERFACE_SIGNAL(void(std::vector<TAlert>*), PopulateAlerts);
};

DEFINE_REFCOUNTED_TYPE(IAlertManager)

//! If a non-default (non-dummy) profiler is specified and alert collectors are used, metrics
//! corresponding to the alert categories and tags will be reported by emitting 0/1 values to a gauge.
IAlertManagerPtr CreateAlertManager(
    NLogging::TLogger logger,
    NProfiling::TProfiler alertProfiler = {},
    IInvokerPtr invoker = GetCurrentInvoker());

////////////////////////////////////////////////////////////////////////////////

//! A helper-class for storing and publishing per-component alerts, as well as reporting alert-based metrics.
//! The purpose of this class is to isolate alert lifetimes across components and logical processes of a service.
//! While the alert manager serves as a central export point of all of the alerts of a service, each collector is
//! responsible for grouping alerts with the same stage-publish cycle, as well as managing the corresponding gauges.
//! Different logical components and regular processes with differing periods should use different collectors.
//! Alerts from different collectors can share the same category, in which case they will be grouped together by the alert manager into a single aggregated alert.
//! This allows grouping alerts from multiple similar user-configurable periodic processes (e.g. exports from queues into static tables).
//!
//! Thread affinity: any.
struct IAlertCollector
    : public TRefCounted
{
public:
    //! Stages and stores alert.
    //! NB: The descriptions of staged alerts should be equal within the same category. This means that you
    //! can use the same category for alerts with different descriptions, but not within the same stage-publish cycle.
    //! Otherwise, it would be unclear what description the alert manager should use when grouping these alerts together.
    virtual void StageAlert(TAlert alert) = 0;
    //! Publishes alerts so they are visible to the alert manager. Reports metrics on all known category + tags combinations.
    //! NB: Stored gauges are persistent. If an alert with some (category, tags) combinations was staged during the lifetime of this class,
    //! 0/1 values will be reported for it up until the collector is destroyed. This is necessary for reporting that an alert is no longer applicable.
    //! NB: Due to the comment above, it is important to keep the number of (category, tags) combinations very reasonably finite, since each of them causes a sensor to be produced.
    virtual void PublishAlerts() = 0;

    // TODO(achulkov2): Implement some mechanism for clearing out unnneeded gauges.
};

DEFINE_REFCOUNTED_TYPE(IAlertCollector)

//! The alert collector will subscribe itself to the manager upon construction and unsubscribe upon destruction.
IAlertCollectorPtr CreateAlertCollector(const IAlertManagerPtr& alertManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAlertManager
