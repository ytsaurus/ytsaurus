#pragma once

#include "public.h"
#include "sensor_set.h"
#include "producer.h"
#include "tag_registry.h"

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/misc/lock_free.h>

#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/impl.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

struct TSensorInfo
{
    TString Name;
    int ObjectCount;
    int CubeSize;
    TError Error;
};

////////////////////////////////////////////////////////////////////////////////

class TSolomonRegistry
    : public IRegistryImpl
{
public:
    explicit TSolomonRegistry();

    virtual ICounterImplPtr RegisterCounter(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    virtual ITimeCounterImplPtr RegisterTimeCounter(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    virtual IGaugeImplPtr RegisterGauge(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    virtual ITimeGaugeImplPtr RegisterTimeGauge(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    virtual ISummaryImplPtr RegisterSummary(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    virtual IGaugeImplPtr RegisterGaugeSummary(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    virtual ITimerImplPtr RegisterTimerSummary(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    virtual ITimerImplPtr RegisterTimerHistogram(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options) override;

    virtual void RegisterFuncCounter(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options,
        const TRefCountedPtr& owner,
        std::function<i64()> reader) override;

    virtual void RegisterFuncGauge(
        const TString& name,
        const TTagSet& tags,
        TSensorOptions options,
        const TRefCountedPtr& owner,
        std::function<double()> reader) override;

    virtual void RegisterProducer(
        const TString& prefix,
        const TTagSet& tags,
        TSensorOptions options,
        const ISensorProducerPtr& owner) override;

    static TSolomonRegistryPtr Get();

    void Disable();
    void SetDynamicTags(std::vector<TTag> dynamicTags);
    std::vector<TTag> GetDynamicTags();

    void SetWindowSize(int windowSize);
    void ProcessRegistrations();
    void Collect(IInvokerPtr offloadInvoker = GetSyncInvoker());
    void ReadSensors(
        const TReadOptions& options,
        ::NMonitoring::IMetricConsumer* consumer) const;

    void ReadRecentSensorValue(
        const TString& name,
        const TTagList& tags,
        const TReadOptions& options,
        NYTree::TFluentAny fluent) const;

    //! LegacyReadSensors sends sensor values to core/profiling.
    void LegacyReadSensors();

    std::vector<TSensorInfo> ListSensors() const;

    const TTagRegistry& GetTags() const;

    i64 GetNextIteration() const;
    int GetWindowSize() const;
    int IndexOf(i64 iteration) const;

    void Profile(const TProfiler& profiler);
    const TProfiler& GetSelfProfiler() const;

private:
    i64 Iteration_ = 0;
    std::optional<int> WindowSize_;
    TProfiler SelfProfiler_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, DynamicTagsLock_);
    std::vector<TTag> DynamicTags_;

    std::atomic<bool> Disabled_ = false;
    TMultipleProducerSingleConsumerLockFreeStack<std::function<void()>> RegistrationQueue_;

    template <class TFn>
    void DoRegister(TFn fn);

    TTagRegistry Tags_;
    TProducerSet Producers_;

    THashMap<TString, TSensorSet> Sensors_;

    TSensorSet* FindSet(const TString& name, const TSensorOptions& options);

    TCounter RegistrationCount_;
    TEventTimer SensorCollectDuration_, ReadDuration_;
    TGauge SensorCount_, ProjectionCount_, TagCount_;
};

DEFINE_REFCOUNTED_TYPE(TSolomonRegistry)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
